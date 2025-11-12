package com.keystar.flink.kr_algorithm

import com.keystar.flink.iotdbfunction.DiagnosisResult                           // 导入诊断结果模型类
import com.keystar.flink.iotdbfunction.IotdbFunction.Diagnostic_method           // 导入诊断方法
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}   // 导入IoTDB数据处理工具
import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}               // 导入规则和IoT数据模型
import com.keystar.flink.kr_algorithm.AlgorithFunction._                        // 导入算法工具类的所有方法
import org.apache.flink.api.common.state._                                      // 导入Flink状态管理相关类
import org.apache.flink.configuration.Configuration                              // 导入Flink配置类
import org.apache.flink.util.{Collector, OutputTag}                             // 导入Flink输出相关类
import org.slf4j.LoggerFactory                                                   // 导入日志工具
import play.api.libs.json._  // 关键导入：引入JsValue类型
// 新增必要的导入
import org.apache.flink.api.scala.createTypeInformation                         // 导入Flink类型信息创建工具
import org.apache.flink.streaming.api.functions.KeyedProcessFunction            // 导入Flink键控处理函数

import java.sql._                                // 导入JDBC相关类
import java.text.SimpleDateFormat                // 导入日期格式化类
import java.time.Instant                        // 导入时间处理类
import java.util.TimeZone                       // 导入时区处理类
import scala.collection.mutable                  // 导入Scala可变集合
import scala.collection.mutable.ListBuffer      // 导入可变列表
import scala.util.matching.Regex                // 导入正则表达式类

class AlgorithmTransformateComplex(timestamp: Long, sitePartitionMap: mutable.Map[String, Int]) extends KeyedProcessFunction[ String,(String,String), AlgorithResult]{

  //定义一个记录规则数据是否加载完成的标识
  private var rulesState: ListState[Map[String, List[AlgorithmDeviceInstance]]] = _

  // 状态管理器，用于存储每个测点的上一个有效值及其时间戳
  // 使用 ValueState，键为测点标识，值为三元组 (值，时间戳，数值)
  private lazy val lastValidValuesState: ValueState[Map[String, (Option[Any], Option[Long], Option[Double])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Any], Option[Long], Option[Double])]]("lastValidValues", classOf[Map[String, (Option[Any], Option[Long], Option[Double])]])
  )

  // 状态管理器，用于记录采样频率不符问题开始的时间戳
  // 值为二元组 (开始时间，结束时间)
  private lazy val samplingFrequencyMismatchStart: ValueState[Map[String, (Option[Long], Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long])]]("samplingFrequencyMismatchStart", classOf[Map[String, (Option[Long], Option[Long])]])
  )

  // 状态管理器，用于记录站点、设备，上次跑的时间
  // 存储每个设备的两次运行时间戳
  private lazy val facilityStartTime: ValueState[Map[String, (Option[Long],Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long],Option[Long])]]("facilityStartTime", classOf[Map[String, (Option[Long],Option[Long])]])
  )

  // 状态管理器，用于记录每个传感器的死值故障状态
  // 每个记录包含死值和对应的时间戳
  private lazy val dedfaultState: ValueState[Map[String, List[(Option[Double], Some[Long])]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, List[(Option[Double], Some[Long])]]]("dedfaultState", classOf[Map[String, List[(Option[Double], Some[Long])]]])
  )


  // 状态管理器，用于记录故障状态
  // 元组包含：最大值超限标志及时间、最小值超限标志及时间、速率异常标志及时间、死值标志、其他故障标志
  private lazy val faultStates: ValueState[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]]("faultStates", classOf[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]])
  )

  // JDBC 连接相关
  @transient private var connection: Connection = _             // 数据库链接
  @transient private var psFull: PreparedStatement = _          // 全量查询语句
  @transient private var psIncremental: PreparedStatement = _   // 增量查询语句
  private var continue_data:MapState[String,List[(Any,Long)]]= _ //持续数据状态

  // 显式定义 Timestamp 的排序规则：用于比较时间戳大小
  implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }

  // 规则数据缓存：用于存储按键分组的算法设备实例
  private lazy val rulesCache: ValueState[Map[String, mutable.ArrayBuffer[AlgorithmDeviceInstance]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, mutable.ArrayBuffer[AlgorithmDeviceInstance]]]("rulesCache", classOf[Map[String, mutable.ArrayBuffer[AlgorithmDeviceInstance]]])
  )

  // 定义站点计数器状态，用于生成分区键
  private lazy val siteCountersState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("siteCounters", classOf[Long])
  )

  // 新增状态：记录每个 key 最后一次定时器的触发时间（处理时间，毫秒）,默认值为
  private lazy val lastTimerTime: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("lastTimerTime", classOf[Long], 0L)
  )


  // 定义存储所有算法设备实例的 ListState
  private var allDataState: ListState[AlgorithmDeviceInstance] = _

  // 定义查询间隔常量（一天）和日志实例
  // 查询间隔（例如每天）
  private val queryIntervalMillis = 24 * 60 * 60 * 1000L // 一天
  private val logger = LoggerFactory.getLogger(this.getClass)

  // 定义诊断规则的可变列表
  private var rule_data:ListBuffer[DiagnosisRule]= _

  //首次加载
  private var isRulesLoadedState: ValueState[Boolean] = _
  // 定义一个新的状态变量，用于存储全局站点偏移量
  private var globalSiteOffsetsState: ValueState[Map[String, Int]] = _


  override def open(parameters: Configuration): Unit = {
    // 初始化状态，指定状态名称和数据类型
    rulesState = getRuntimeContext.getListState(
      new ListStateDescriptor[Map[String, List[AlgorithmDeviceInstance]]]("AlgorithmDeviceInstance", classOf[Map[String, List[AlgorithmDeviceInstance]]])
    )

    val descriptor = new ListStateDescriptor[AlgorithmDeviceInstance](
      "allDataState", // 状态名称
      createTypeInformation[AlgorithmDeviceInstance] // 数据类型
    )
    // 获取键控状态
    allDataState = getRuntimeContext.getListState(descriptor)  // 初始化allDataState

    val lastread = new MapStateDescriptor[String, List[(Any, Long)]](
      "lastreadIotDB",
      classOf[String],
      classOf[List[(Any, Long)]]
    )
    // 初始化continue_data
    continue_data= getRuntimeContext.getMapState(lastread)

    // 初始化 isRulesLoadedState，默认值为 false，表示规则未加载
    val isRulesLoade = new ValueStateDescriptor[Boolean]("isRulesLoaded", classOf[Boolean], false)
    isRulesLoadedState = getRuntimeContext.getState(isRulesLoade)

    // 初始化数据库连接
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"

    Class.forName(driver)                                                  // 加载驱动
    connection = DriverManager.getConnection(url, user, password)          // 创建连接
    connection.setAutoCommit(true)                                         // 设置自动提交



    // 准备 SQL
    // 全量查询算法规则的sql语句
    val fullQuery =s"""
                      |SELECT
                      |id,
                      |algorithm_id,
                      |product_id,
                      |equip_label,
                      |alg_user_editable,
                      |alg_is_realtime,
                      |alg_duration_seconds,
                      |alg_param,
                      |alg_input,
                      |alg_formula_intermediate,
                      |alg_formula_final,
                      |create_time,
                      |update_time,
                      |site_id,
                      |site_name
                      |FROM public.kr_algorithm_device_instance
                      |WHERE alg_is_realtime = true
                      |ORDER BY algorithm_id
                      |""".stripMargin

    psFull = connection.prepareStatement(fullQuery)
    psFull.setFetchSize(1000)

    // 增量查询算法规则的sql语句（带时间条件）
    val incrementalQuery =s"""
                             |SELECT
                             |id,
                             |algorithm_id,
                             |product_id,
                             |equip_label,
                             |alg_user_editable,
                             |alg_is_realtime,
                             |alg_duration_seconds,
                             |alg_param,
                             |alg_input,
                             |alg_formula_intermediate,
                             |alg_formula_final,
                             |create_time,
                             |update_time,
                             |site_id,
                             |site_name
                             |FROM public.kr_algorithm_device_instance
                             |WHERE alg_is_realtime = true AND update_time > ?
                             |ORDER BY algorithm_id
                             |""".stripMargin

    psIncremental = connection.prepareStatement(incrementalQuery)
    psIncremental.setFetchSize(1000)
  }



  // 增量刷新
  // 定义增量刷新则方法
  def refreshRulesData(key: String): Unit = {
    // 获取前规则缓存
    val currentRules = Option(rulesCache.value())
      .map(_.getOrElse(key, mutable.ArrayBuffer.empty[AlgorithmDeviceInstance]))  // 提取当前key对应的规则
      .getOrElse(mutable.ArrayBuffer.empty[AlgorithmDeviceInstance])              // 默认为空
    val validTimestamps: Iterable[Timestamp] = currentRules   // 提取有效时间戳
      .map(_.updateTime)
      .filter(_ != null)
    // 2. 手动计算最大值（处理空集合情况）
    val maxTime: Option[Timestamp] = if (validTimestamps.nonEmpty) {
      Some(validTimestamps.max)   // 计算最大时间戳
    } else None

    // 异常处理块
    // 处理 key，提取表名用于后续查询
    try {
      // 查询全量数据
      //要截取key再获取数据
      val parts = key.split("_")        // 分割key
      val tableKey = if (parts.length > 1) {    // 提取表名
        parts.init.mkString("_") // 所有除最后一个部分外拼接回来
      } else {
        key
      }

      // 2. 将字符串时间转换为 Timestamp
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("GMT+8")) // 设置解析时区

      val defaultTimeStr = "2025-08-07 08:00:00"     // 默认时间字符串
      val defaultMillis = sdf.parse(defaultTimeStr).getTime   // 转换为毫秒
      val defaultTime = maxTime.getOrElse(new Timestamp(defaultMillis))  // 默认时间戳


      psIncremental.setTimestamp(1, defaultTime)    // 设置增量查询的时间参数

      //获取数据
      // 5. 处理查询结果，过滤有效规则
      val newRules = mutable.ArrayBuffer[AlgorithmDeviceInstance]()   // 存储新规则
      var resultSet: ResultSet = null   // 结果集变量
      try {
        resultSet = psIncremental.executeQuery()     // 执行增量查询
        // 如果结果集不为空
        if (resultSet != null) {
          // 遍历结果
          while (resultSet.next()) {
            // 创建规则对象、AlgorithmDeviceInstance实例
            val rule = AlgorithmDeviceInstance(
              id = resultSet.getLong("id"),
              algorithm_id = resultSet.getLong("algorithm_id"),
              productId = Option(resultSet.getLong("product_id")).filter(_ != 0), // 处理NULL
              equip_label = resultSet.getString("equip_label"),
              algUserEditable = resultSet.getBoolean("alg_user_editable"),
              algIsRealtime = resultSet.getBoolean("alg_is_realtime"),
              algDurationSeconds = resultSet.getInt("alg_duration_seconds"),
              algParam = Option(resultSet.getString("alg_param")),
              alg_input=Option(resultSet.getString("alg_input")).map(Json.parse),
              algFormulaIntermediate = Option(resultSet.getString("alg_formula_intermediate")).map(Json.parse),
              algFormulaFinal = Option(resultSet.getString("alg_formula_final")).map(Json.parse),
              createTime = resultSet.getTimestamp("create_time"),
              updateTime = resultSet.getTimestamp("update_time"),
              siteId = Option(resultSet.getLong("site_id")).filter(_ != 0),
              siteName = Option(resultSet.getString("site_name"))
            )

            // 判断是否应该添加新规则
            val shouldAddRule = rule.algFormulaFinal.exists { formulaFinal =>
              // 1. 提取 "formula_final" 字段并尝试转为 JsArray
              (formulaFinal \ "formula_final").asOpt[JsArray].exists { jsArray =>

                // 2. 遍历数组中的每个元素
                jsArray.value.exists { item =>
                  // 3. 提取 position 和 output
                  val position = (item \ "position").asOpt[String].getOrElse("").trim
                  val outputExpr = (item \ "output").asOpt[String].getOrElse("")

                  // 4. 判断条件：position不为空 且 output包含 "_isActPowerLow"
                  position.nonEmpty && outputExpr.contains("_kryj")
                }
              }
            }

            // 如果满足上面的条件，添加到新规则列表
            if (shouldAddRule) {
              newRules += rule
            }
          }
          // 获取当前缓存中所有分区的规则（全量分区键）
          val currentRulesMap = Option(rulesCache.value()).getOrElse(Map.empty)
          // 提取所有现有分区键（用于生成新规则的分区）
          val existingPartitionKeys = currentRulesMap.keySet.toList
          // 6. 对每条新规则计算其应归属的分区key（核心：复用全量的分区策略）
          // 如果有新规则
          if (newRules.nonEmpty) {
            // 获取当前计数器（与全量逻辑保持一致的计数器状态）
            val currentCounter = Option(siteCountersState.value()).getOrElse(0L)   // 当前计数器值
            var updatedCounter = currentCounter                                    // 更新后的计数器

            // 遍历新规则，计算分区并追加
            val updatedRulesMap = newRules.foldLeft(currentRulesMap) { (accMap, rule) =>
              // 用与全量相同的方法生成分区key
              val siteId = rule.siteId.getOrElse(0L)    // 站点ID
              val partitionKey = generatePartitionKey(siteId.toString, updatedCounter, existingPartitionKeys)  // 生成分区键

              // 获取该分区当前的规则集合
              val partitionRules = accMap.getOrElse(partitionKey, mutable.ArrayBuffer.empty)  // 分区规则

              // 去重（避免重复添加）
              // 如果规则不存在，则添加规则
              if (!partitionRules.exists(_.id == rule.id)) {
                val newPartitionRules = (partitionRules :+ rule).sortBy(_.id)   // 添加规则并排序
                updatedCounter += 1 // 每添加一条规则，计数器自增（与全量逻辑一致）
                accMap.updated(partitionKey, newPartitionRules)
              } else {
                accMap // 已存在规则，不更新
              }
            }

            // 7. 更新缓存和计数器
            rulesCache.update(updatedRulesMap)    // 更新缓存
            siteCountersState.update(updatedCounter)    // 更新计数器
            logger.info(s"增量刷新完成，新增规则 ${newRules.size} 条，更新后分区数 ${updatedRulesMap.size}")
          }
        }
      }
      // 合并到缓存
    } catch {
      case e: Exception =>
        println(s"查询规则数据出错: ${e.getMessage}")
    }
  }



  // 辅助方法：为特定设备的站点构建局部偏移量映射
  private def buildDeviceSiteOffsets(deviceSites: List[String]): Map[String, Int] = {
    // 过滤相关站点
    val deviceSitePartitionMap = sitePartitionMap.filterKeys(deviceSites.contains)
    // 偏移量初始值
    var offset = 0
    deviceSitePartitionMap.toList.flatMap { case (siteId, numPartitions) =>   // 构建偏移量映射
      val result = (siteId, offset)    // 站点id与偏移量的映射
      offset += numPartitions         // 累加分区数
      List(result)                   // 返回结果
    }.toMap                         // 转换为map
  }

  // 在类中添加一个辅助方法，用于统一生成分区键
  private def generatePartitionKey(siteId: String, counter: Long, deviceSites: List[String]): String = {
    // 为当前设备的站点构建局部偏移量映射
    val deviceSiteOffsets = buildDeviceSiteOffsets(deviceSites)

    // 获取站点分区数
    val numPartitions = Math.max(sitePartitionMap.getOrElse(siteId, 1) / 2, 1)


    // 确保计数器不超过分区数
    val adjustedCounter = counter % numPartitions

    // 生成最终分区键
    val offset = deviceSiteOffsets.getOrElse(siteId, 0)
    s"${siteId}_${offset + adjustedCounter}"
  }

  //全量查询获取
  def fetchRules(fullSql: String, key: String): Unit = {
    var resultSet: ResultSet = null
    try {
      val statement = connection.prepareStatement(fullSql)
      statement.setFetchSize(1000)
      resultSet = statement.executeQuery()

      // 收集当前设备相关的所有站点
      val deviceSites = mutable.Set[String]()
      val tempRules = mutable.ArrayBuffer[AlgorithmDeviceInstance]()

      // 第一遍扫描：收集站点信息
      while (resultSet.next()) {
        val rule = AlgorithmDeviceInstance(
          id = resultSet.getLong("id"),
          algorithm_id = resultSet.getLong("algorithm_id"),
          productId = Option(resultSet.getLong("product_id")).filter(_ != 0), // 处理NULL
          equip_label = resultSet.getString("equip_label"),
          algUserEditable = resultSet.getBoolean("alg_user_editable"),
          algIsRealtime = resultSet.getBoolean("alg_is_realtime"),
          algDurationSeconds = resultSet.getInt("alg_duration_seconds"),
          algParam = Option(resultSet.getString("alg_param")),
          alg_input=Option(resultSet.getString("alg_input")).map(Json.parse),
          algFormulaIntermediate = Option(resultSet.getString("alg_formula_intermediate")).map(Json.parse),
          algFormulaFinal = Option(resultSet.getString("alg_formula_final")).map(Json.parse),
          createTime = resultSet.getTimestamp("create_time"),
          updateTime = resultSet.getTimestamp("update_time"),
          siteId = Option(resultSet.getLong("site_id")).filter(_ != 0),
          siteName = Option(resultSet.getString("site_name"))
        )
        val siteIdValue = rule.siteId.getOrElse(0L)  // 如果siteId为None，返回0L
        val siteIdStr = s"root.ln.`$siteIdValue`"

        // 检查是否满足过滤条件
        val shouldAddRule = rule.algFormulaFinal.exists { formulaFinal =>
          // 1. 提取 "formula_final" 字段并尝试转为 JsArray
          (formulaFinal \ "formula_final").asOpt[JsArray].exists { jsArray =>
            // 2. 遍历数组中的每个元素
            jsArray.value.exists { item =>
              // 3. 提取 position 和 output
              val position = (item \ "position").asOpt[String].getOrElse("").trim
              val outputExpr = (item \ "output").asOpt[String].getOrElse("")

              // 4. 判断条件：position不为空 且 output包含 "_isActPowerLow"
              position.nonEmpty && outputExpr.contains("kryj")
            }
          }
        }

        // 满足条件才加入规则集合
        if (shouldAddRule) {
          deviceSites += siteIdStr
          tempRules += rule
        }
      }
      println(tempRules.size)
      val siteSet = sitePartitionMap.keySet.toList
      // 转换为不可变List供后续使用
      val deviceSitesList = deviceSites.toList

      // 第二遍处理：应用局部偏移量生成正确的分区键
      for (rule <- tempRules) {
        val siteId = rule.siteId.getOrElse(0L)

        // 获取当前计数器值，处理null
        val counter = Option(siteCountersState.value()).getOrElse(0L)

        // 使用统一的分区键生成方法
        val partitionKey = generatePartitionKey(siteId.toString, counter, siteSet)

        // 获取当前规则缓存
        val currentRulesMap = Option(rulesCache.value()).getOrElse(Map.empty)

        // 获取或创建当前分区的规则集合
        val currentRules = currentRulesMap.getOrElse(
          partitionKey,
          mutable.ArrayBuffer.empty[AlgorithmDeviceInstance]
        )

        // 添加规则（如果不存在）
        if (!currentRules.exists(_.id == rule.id)) {
          currentRules += rule
          currentRules.sortBy(_.id)

          // 更新规则缓存
          val updatedRulesMap = currentRulesMap.updated(partitionKey, currentRules)
          rulesCache.update(updatedRulesMap)

          // 更新计数器
          siteCountersState.update(counter + 1)

//          logger.info(s"成功添加规则=${partitionKey}，新规则数=${currentRules.size}，新计数器=${counter + 1}")
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"获取规则数据出错: ${e.getMessage}", e)
    } finally {
      if (resultSet != null) resultSet.close()
    }
  }



  def getSqlQuery(lastTsOpt: Option[Long], iotFlds: String, device: String, lastTsOpt02: Option[Long]): String = {
    val parts = device.split("_")
    val tableKey = if (parts.length > 1) {
      parts.init.mkString("_") // 所有除最后一个部分外拼接回来
    } else {
      "root.ln.`"+device+"`"
    }
    //    println(s"进来的点位主键key:$tableKey")
    (lastTsOpt, lastTsOpt02) match {
      case (Some(lastTs), Some(lastTs2)) if lastTs == lastTs2 =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(0).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(180).toEpochMilli
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case (Some(lastTs), Some(lastTs2)) if lastTs < lastTs2 =>
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${lastTs} AND time < ${lastTs2}"
      case (Some(lastTs), _) =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(0).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(180).toEpochMilli
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case _ =>
//        s"SELECT ${iotFlds} FROM ${tableKey} ORDER BY time desc LIMIT 60 "
        s"SELECT ${iotFlds} FROM ${tableKey}  where time>=${timestamp} ORDER BY time asc LIMIT 1 "
    }
  }


  def processAndOutput(
                        proData: Seq[(String, String, Option[Any], Long, String)],
                        outputMap: Map[String, (Int, Boolean, Long, String)],
                        key: String,
                        out: Collector[AlgorithResult]
                      ): Unit = {

    proData.foreach { case (_, _, maybeValue, timestamp, position) =>
      outputMap.get(position).foreach { case (durationSeconds, durationStatus, algorithm_id, equip_label) =>
        maybeValue.foreach { collectedValue =>
          // 转换为布尔值（判断当前是否满足告警条件）
          val valueAsBoolean = collectedValue match {
            case bool: Boolean => bool
            case num: Number => num.intValue() == 1 || num.intValue() == 2
            case _ => false
          }

          if(position=="C6_19_isActPowerLow"){
            println(s"C6_19_isActPowerLow:$valueAsBoolean")
          }

          if (!durationStatus) {
            // 无需持续检查：直接输出（逻辑不变）
            val alarm = valueAsBoolean
            out.collect(AlgorithResult(
              timestamp = timestamp,
              expressions = position,
              collectedValue = Some(collectedValue),
              alarm =false,
              station = key,
              equip_label = equip_label,
              algorithm_id = algorithm_id
            ))
          } else {
            // 需要持续检查：调整状态管理逻辑
            val expKey = s"$key-$position"
            val requiredPoints = durationSeconds / 15  // 所需点数（每15秒一个点）

            // 1. 安全获取当前状态（不存在则为空列表）
            val currentState = Option(continue_data.get(expKey)).getOrElse(Nil)

            if (!valueAsBoolean) {
              // 2. 当前值为false：检查是否需要输出最后一次告警，然后清空状态
              if (currentState.nonEmpty) {
                // 若历史数据长度达到所需点数，输出一次告警（视为结束前的最后一次）
                if (currentState.length == requiredPoints) {
                  out.collect(AlgorithResult(
                    timestamp = timestamp,
                    expressions = position,
                    collectedValue = Some(collectedValue),
                    alarm = true,
                    station = key,
                    equip_label = equip_label,
                    algorithm_id = algorithm_id
                  ))
                }
                continue_data.remove(expKey)  // 清空状态
              }
              // 输出当前非告警
              out.collect(AlgorithResult(
                timestamp = timestamp,
                expressions = position,
                collectedValue = Some(collectedValue),
                alarm = false,
                station = key,
                equip_label = equip_label,
                algorithm_id = algorithm_id
              ))
            } else {
              // 3. 当前值为true：处理状态更新与长度适配
              // 3.1 清理超时数据（仅保留cutoffTime之后的）
              val cutoffTime = timestamp - durationSeconds * 1000L  // 时间窗口下限（毫秒）
              val cleanedState = currentState.filter { case (_, ts) => ts >= cutoffTime }

              // 3.2 限制列表长度不超过requiredPoints（只保留最新的N个点）
              val truncatedState = if (cleanedState.length > requiredPoints) {
                cleanedState.takeRight(requiredPoints)  // 超过则截取最新的requiredPoints个
              } else {
                cleanedState
              }

              // 3.3 判断数据连续性（与上一个点间隔是否为15秒）
              val isContinuous = truncatedState.lastOption match {
                case Some((_, lastTs)) => (timestamp - lastTs) == 15 * 1000L
                case None => true  // 第一个点视为连续
              }

              if (isContinuous) {
                // 3.4 连续数据：更新状态（添加当前点，再截断到最大长度）
                val tempState = truncatedState :+ (collectedValue -> timestamp)
                val updatedState = if (tempState.length > requiredPoints) tempState.takeRight(requiredPoints) else tempState
                continue_data.put(expKey, updatedState)

                // 3.5 判断是否满足告警条件（长度达标且所有值有效）
                val allValuesValid = updatedState.forall { case (v, _) =>
                  v match {
                    case b: Boolean => b
                    case n: Number => n.intValue() == 1 || n.intValue() == 2
                    case _ => false
                  }
                }
                val alarm = updatedState.length >= requiredPoints && allValuesValid
                out.collect(AlgorithResult(
                  timestamp = timestamp,
                  expressions = position,
                  collectedValue = Some(collectedValue),
                  alarm = alarm,
                  station = key,
                  equip_label = equip_label,
                  algorithm_id = algorithm_id
                ))
              } else {
                // 3.6 不连续：重置状态为当前点（长度=1）
                // 先输出
                out.collect(AlgorithResult(
                  timestamp = timestamp,
                  expressions = position,
                  collectedValue = Some(collectedValue),
                  alarm = false,
                  station = key,
                  equip_label = equip_label,
                  algorithm_id = algorithm_id
                ))
                // 清空历史状态，再添加状态
                val resetState = List((collectedValue -> timestamp))
                continue_data.remove(expKey)    // 先清空历史状态
                continue_data.put(expKey, resetState)    // 再添加状态

              }
            }
          }
        }
      }
    }
  }

  override def processElement(key: (String,String), context: KeyedProcessFunction[String,(String,String), AlgorithResult]#Context, collector: Collector[AlgorithResult]): Unit = {
    val isRulesLoaded = isRulesLoadedState.value()
    val parts = key._2.split("_")
    val tableKey = if (parts.length > 1) {
      parts.init.mkString("_") // 所有除最后一个部分外拼接回来
    } else {
      key._2
    }
    if (!isRulesLoaded) {
      // 假设tableKey可转为Long
      val fuSql = psFull.toString
      fetchRules(fuSql,key._2)
      rule_data=get_data(tableKey)
      isRulesLoadedState.update(true)
    }else{
      // 已加载规则后，检查是否需要注册定时器
      val currentLastTimer = lastTimerTime.value()
      val currentTime = System.currentTimeMillis()
      // 如果从未注册过定时器，或上一次定时器已过期（避免遗漏），则注册新定时器
      if (currentLastTimer == 0 || currentLastTimer < currentTime) {
        val firstTimerTime = currentTime + 60*60* 1000L // 首次触发时间：1小时后
        context.timerService().registerProcessingTimeTimer(firstTimerTime)
        lastTimerTime.update(firstTimerTime)
        logger.info(s"为 key: ${key._2} 注册首次1小时增量触发器，触发时间: $firstTimerTime")
      }
      //设备时间
      val faStartime = Option(facilityStartTime.value()).getOrElse(Map.empty[String, (Some[Long],Some[Long])])
      //通过key获取到所有的数值

      //防止空指针异常
      val rulesForDevice =  Option(rulesCache.value())
        .flatMap(_.get(key._2))
        .getOrElse(mutable.ArrayBuffer.empty[AlgorithmDeviceInstance])

//        rulesCache.value().getOrElse(key._2, mutable.ArrayBuffer.empty[AlgorithmDeviceInstance])
      //直接查找
      logger.info(s"打印key:${key._2} ${Thread.currentThread().getName} 长度值：${rulesForDevice.size}")
      if(rulesForDevice.nonEmpty){
        //开始提取字段
        //字段以数值之间的关系匹配
        val algData: mutable.Seq[Option[JsValue]] = rulesForDevice.map(_.algFormulaIntermediate).filter(data => data.nonEmpty)
        val algfinal: mutable.Seq[Option[JsValue]] = rulesForDevice.map(_.algFormulaFinal).filter(data => data.nonEmpty)

        //获取json数据
        val algInputJson: mutable.Seq[Option[JsValue]] = rulesForDevice.map(_.alg_input).filter(data => data.nonEmpty)

        val allTables: Seq[String] = algInputJson.flatMap { optJsValue =>
          optJsValue.toSeq.flatMap { jsValue =>
            (jsValue \ "input").asOpt[JsArray].toSeq.flatMap { inputArray =>
              inputArray.value.flatMap { inputElem =>
                (inputElem \ "table").asOpt[String]
              }
            }
          }
        }

        // 去重并转为逗号分隔的字符串
        val uniqueTables = allTables.distinct.mkString(",")

        // 提取 algFormulaIntermediate 中的所有数组元素
        val intermediateSeq: Seq[JsValue] = algData.flatMap {
          // 处理外层Option存在的情况
          case Some(jsValue) =>
            // 提取formula_final字段并转为JsArray
            (jsValue \ "formula_final").asOpt[JsArray] match {
              case Some(jsArray) => jsArray.value  // 提取数组元素
              case None => Seq.empty             // 字段不存在或类型错误
            }
          // 处理外层Option为None的情况
          case None =>
            Seq.empty
        }


        // 提取 algFormulaFinal 中的所有数组元素（逻辑同上）

        import play.api.libs.json._

        val finalSeq: Seq[JsValue] = algfinal.flatMap {
          // 处理外层Option存在的情况
          case Some(jsValue) =>
            // 提取formula_final字段并转为JsArray
            (jsValue \ "formula_final").asOpt[JsArray] match {
              case Some(jsArray) => jsArray.value  // 提取数组元素
              case None => Seq.empty             // 字段不存在或类型错误
            }
          // 处理外层Option为None的情况
          case None =>
            Seq.empty
        }

        // （可选）合并 intermediate 和 final 的结果
        val allData: Seq[JsValue] = (intermediateSeq ++ finalSeq)
        // 合并两个集合，过滤掉 None 值，提取 JsValue
//        val allData: Seq[JsValue] = (algData ++ algfinal).flatten

        // 定义正则表达式
        val pattern: Regex = """kryj\['([^']+)'\]""".r

        // 从所有 JSON 对象的 "output" 字段中提取点位  可能存在json嵌套json的关系
        val pointIds: Seq[String] = allData.flatMap { json =>
          (json \ "output").asOpt[String].toList.flatMap { outputStr =>
            pattern.findAllMatchIn(outputStr).map(_.group(1)).toList
          }
        }

        logger.info(s"打印数值：${pointIds.size}")

        //获取对应的站点名称
        val siteId: List[Option[Long]] = rulesForDevice.map(_.siteId).toList.distinct
        val siteIds = siteId.flatten.distinct.toList.mkString("")
        // 根据是否是首次执行决定时间戳
        val (lastTimestampOpt, lastTimestampOpt2) = faStartime.get(key._2) match {
          case Some((t1, t2)) => (t1, t2)
          case None => (None, None)
        }
        //取出带有_vaild的值
        val validPointIds = pointIds.distinct.filter(_.endsWith("_valid"))
        val validupdaterpose =if(validPointIds.nonEmpty){
          //获取原始点位
          val singedata = validPointIds.map { data =>
            // 使用正则表达式替换掉 "_dataphysics" 或 "_datasample" 后的部分
            val regex = "_valid|_basicerr".r
            val result = regex.split(data)
            if (result.nonEmpty) result.head else ""
          }.toSet.mkString(",")
          //获取规则数据
          val rulesForDevice: ListBuffer[DiagnosisRule] = filterRules(rule_data,singedata, uniqueTables)
          val validiotSql = getSqlQuery(lastTimestampOpt, singedata, uniqueTables,lastTimestampOpt2)
          val validresult = sendRequest(validiotSql)
          //调整复杂vaild值的逻辑
          val validresponses = handleResponse(validresult)
          val reponse= validresponses.map(response => updateResponse(response, rulesForDevice,context))
          //更新主键值
          replaceKeysAndValues(reponse)
        }else List.empty[IoTDBReading]

        //筛选非有效值
        val postData = pointIds.distinct.filter { input =>
          !(input.contains("_valid") || input.endsWith("_basicerr"))
        }

        val sql = getSqlQuery(lastTimestampOpt, postData.mkString(","), uniqueTables,lastTimestampOpt2)
        logger.info(s"涉及的key：${key._2} 打印时间:${lastTimestampOpt} ${lastTimestampOpt2} 对应的sql2: ${sql} 线程名称: ${Thread.currentThread().getName}")

        val result = sendRequest(sql)
        val response = handleResponse(result)
        val responses = response++validupdaterpose
        val timestamps = responses.map(_.timestamp)
        // 获取最大时间戳（Option）
        val maxTimestamp = if (timestamps.nonEmpty) Some(timestamps.max) else None
        if(maxTimestamp.isEmpty||maxTimestamp.get==lastTimestampOpt.getOrElse(0L)){
          val lastTimestampOpts = lastTimestampOpt2.map(_ + 300000)
          facilityStartTime.update(Map(key._2 -> (lastTimestampOpt, lastTimestampOpts)))
        }else{
          //说明有值并且不是重复数值
          facilityStartTime.update(Map(key._2 -> (maxTimestamp,maxTimestamp)))
          //得到结果值
          val proData: Seq[(String, String, Option[Any], Long, String)] = processResponses(responses, allData.distinct)
          //获取输出点与持续时长问题
          val outputMap= buildPositionDurationMap(rulesForDevice.toList)
          processAndOutput(proData.distinct,outputMap,siteIds,collector)
        }
      }
    }
  }
  // 重写 onTimer 方法，处理定时触发逻辑
  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String,(String,String), AlgorithResult]#OnTimerContext,
                        out: Collector[AlgorithResult]
                      ): Unit = {
    // 1. 执行增量刷新规则的逻辑（调用你的 refreshRulesData 方法）
    val currentKey = ctx.getCurrentKey // 获取当前 key
    refreshRulesData(currentKey)
    logger.info(s"key: $currentKey 触发1小时增量刷新，时间戳: $timestamp")

    // 2. 注册下一次定时器（当前时间 + 1小时）
    val nextTimerTime = System.currentTimeMillis() +  60*60 * 1000L // 1小时（毫秒）
    ctx.timerService().registerProcessingTimeTimer(nextTimerTime)
    lastTimerTime.update(nextTimerTime) // 更新状态中的最后一次定时器时间
  }

  private def updateResponse(response: IoTDBReading, rulesForDevice: ListBuffer[DiagnosisRule], context: KeyedProcessFunction[String,(String,String), AlgorithResult]#Context): IoTDBReading = {
    // 获取状态值，若为空则初始化为空 Map
    val mismatchStart2 = Option(samplingFrequencyMismatchStart.value()).getOrElse(Map.empty[String, (Option[Long], Option[Long])])
    val faultStatesMap2 = Option(faultStates.value()).getOrElse(Map.empty[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)])
    val dedfaultstate2 = Option(dedfaultState.value()).getOrElse(Map.empty[String, List[(Option[Double], Some[Long])]])
    val lastValidValues2 = Option(lastValidValuesState.value()).getOrElse(Map.empty[String, (Option[Any], Option[Long], Option[Double])])

    // 用于存储更新后的响应值
    var updatedResponse = response

    // 遍历响应中的每个键值对
    for ((key, value) <- response.values) {
      // 检查值是否存在且不为 null
      if (value.exists(_ != null)) {
        try {
          // 处理 IoT 数据，得到诊断结果
          val diagnosisResult: DiagnosisResult = handleIotDBdata(response, rulesForDevice, lastValidValues2, mismatchStart2, faultStatesMap2, dedfaultstate2)

          // 更新响应对象
          updatedResponse = updatedResponse.copy(values = Map(diagnosisResult.expressions -> diagnosisResult.validValue))

          // 发送诊断结果到侧输出流
        } catch {
          // 处理 handleIotDBdata 方法可能抛出的异常
          case e: Exception =>
            println(s"Error handling IoT data for key $key: ${e.getMessage}")
        }
      }
    }
    // 返回最终更新后的响应对象
    updatedResponse
  }

  def handleIotDBdata(data: IoTDBReading,
                      rulesForDevice: Iterable[DiagnosisRule],
                      lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                      mismatchStart: Map[String, (Option[Long], Option[Long])],
                      faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                      dedfaultstate: Map[String, List[(Option[Double], Some[Long])]]
                     ): DiagnosisResult = {
    var diagnosisResult = DiagnosisResult(timestamp = 0, expressions = ""
      , collectedValue = None, faultDescription = ""
      , validValue = None, lastValidTimestamp = None)
    for ((key, value) <- data.values) {
      // 使用 . 分割字符串
      val parts = key.split("\\.")
      // 获取最后一部分即测点
      val result = parts.lastOption.getOrElse("")
      val rule = rulesForDevice.find(_.iot_fld == result)
      rule match {
        case Some(r) =>
          val (lastValue, samFrStart, faultMap, dedValues, sampCheck) = Diagnostic_method(key = key, value = value, iotData = data, rule = r
            , lastValidValues = lastValidValues
            , samplingFrequencyMismatchStart = mismatchStart
            , faultStatesMap = faultStatesMap
            , dedValidValues = dedfaultstate)
          lastValidValuesState.update(lastValue)
          samplingFrequencyMismatchStart.update(samFrStart)
          faultStates.update(faultMap)
          dedfaultState.update(dedValues)
          //          println(s"打印死值的数据：$dedValues")
          val (maxbool, _, _, minbool, _, _, ratebool, _, _, dedbool, lastbool) = faultMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))
          var faultDescription = "0b0000"
          if (sampCheck) {
            faultDescription = "0b010000"
          } else if (maxbool) {
            faultDescription = "0b1000"
          } else if (minbool) {
            faultDescription = "0b0100"
          } else if (ratebool) {
            faultDescription = "0b1001"
          } else if (dedbool) {
            faultDescription = "0b0001"
          } else if (lastbool) {
            faultDescription = "0b1111"
          } else {
            faultDescription = "0b0000"
          }
          val validValue =if(faultDescription == "0b010000") lastValidValues.getOrElse(key, (None, None, None))._3
          else lastValue.getOrElse(key, (None, None, None))._3
          val lastTimestamp2 = lastValidValues.getOrElse(key, (None, None, None))._2
          diagnosisResult = DiagnosisResult(
            timestamp = data.timestamp,
            expressions = key,
            collectedValue = value,
            faultDescription = faultDescription,
            validValue = validValue,
            lastValidTimestamp = lastTimestamp2
          )
        case None =>
          val lastTimestamp2: Option[Long] = lastValidValues.getOrElse(key, (None, None, None))._2
          if (lastTimestamp2.isDefined) {
            val updatedValues = lastValidValues + (key -> (lastValidValues.getOrElse(key, (None, None, None))._1, Some(data.timestamp), None))
            lastValidValuesState.update(updatedValues)
            //测点存在上一条数据
            diagnosisResult = DiagnosisResult(
              timestamp = data.timestamp,
              expressions = key,
              collectedValue = value,
              faultDescription = "",
              validValue = lastValidValues.getOrElse(key, (None, None, None))._1,
              lastValidTimestamp = lastTimestamp2
            )
          } else {
            // 如果没有上一个时间戳，说明这是第一个数据点，直接记录当前时间戳
            val updatedValues = lastValidValues + (key -> (None, Some(data.timestamp), None))
            lastValidValuesState.update(updatedValues)
            //没有该节点的规则
            diagnosisResult = DiagnosisResult(
              timestamp = data.timestamp,
              expressions = key,
              collectedValue = value,
              faultDescription = "",
              validValue = None,
              lastValidTimestamp = None
            )
          }
      }
    }
    diagnosisResult
  }

  override def close(): Unit = {
    if (psIncremental != null) psIncremental.close()
    if (psFull != null) psFull.close()
    if (connection != null) connection.close()
  }
}
