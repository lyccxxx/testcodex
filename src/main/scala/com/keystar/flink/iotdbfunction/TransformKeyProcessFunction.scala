package com.keystar.flink.iotdbfunction

import com.keystar.flink.iotdbfunction.IotdbFunction.{Diagnostic_method, createDiagnosisRuleFromResultSet}
import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import com.keystar.flink.kr_json.InputData
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import com.keystar.flink.kr_json.{InputData, JsonFormats}
import com.keystar.flink.kr_json.JsonFormats.RuleWrapper

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.time.{Instant, LocalDateTime}
import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.math.Ordering.comparatorToOrdering
import org.apache.flink.streaming.api.scala._
import java.io.File
import play.api.libs.json.Json
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone




class TransformKeyProcessFunction (timestamp: Long,sitePartitionMap: mutable.Map[String, Int])  extends KeyedProcessFunction[ String,(String,String), DiagnosisResult]{

  //定义一个记录规则数据是否加载完成的标识
  private var rulesState: ListState[Map[String, List[DiagnosisRule]]] = _

  // 状态管理器，用于存储每个测点的上一个有效值及其时间戳
  private lazy val lastValidValuesState: ValueState[Map[String, (Option[Any], Option[Long], Option[Double])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Any], Option[Long], Option[Double])]]("lastValidValues", classOf[Map[String, (Option[Any], Option[Long], Option[Double])]])
  )

  // 状态管理器，用于记录采样频率不符问题开始的时间戳
  private lazy val samplingFrequencyMismatchStart: ValueState[Map[String, (Option[Long], Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long])]]("samplingFrequencyMismatchStart", classOf[Map[String, (Option[Long], Option[Long])]])
  )

  // 状态管理器，用于记录站点、设备，上次跑的时间
  private lazy val facilityStartTime: ValueState[Map[String, (Option[Long],Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long],Option[Long])]]("facilityStartTime", classOf[Map[String, (Option[Long],Option[Long])]])
  )

  // 状态管理器，用于记录每个传感器的死值故障状态
  private lazy val dedfaultState: ValueState[Map[String, List[(Option[Double], Some[Long])]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, List[(Option[Double], Some[Long])]]]("dedfaultState", classOf[Map[String, List[(Option[Double], Some[Long])]]])
  )


  // 状态管理器，用于记录故障状态
  private lazy val faultStates: ValueState[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]]("faultStates", classOf[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]])
  )

  // JDBC 连接相关
  @transient private var connection: Connection = _
  @transient private var psFull: PreparedStatement = _
  @transient private var psIncremental: PreparedStatement = _

  // 显式定义 Timestamp 的排序规则
  implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }

  // 状态用于保存上次查询时间
  private lazy val lastQueryTime: ValueState[LocalDateTime] = getRuntimeContext.getState(
    new ValueStateDescriptor[LocalDateTime]("lastQueryTime", classOf[LocalDateTime])
  )

  // 规则数据缓存
  private lazy val rulesCache: ValueState[Map[String, mutable.ArrayBuffer[DiagnosisRule]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, mutable.ArrayBuffer[DiagnosisRule]]]("rulesCache", classOf[Map[String, mutable.ArrayBuffer[DiagnosisRule]]])
  )

  private lazy val siteCountersState: MapState[String, Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("siteCounters", classOf[String], classOf[Long])
  )

  // 新增状态：记录每个 key 最后一次定时器的触发时间（处理时间，毫秒）
  private lazy val lastTimerTime: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("lastTimerTime", classOf[Long], 0L)
  )

  private var allDataState: ListState[InputData] = _

  // 查询间隔（例如每天）
  private val queryIntervalMillis = 24 * 60 * 60 * 1000L // 一天
  private val logger = LoggerFactory.getLogger(this.getClass)

  //首次加载
  private var isRulesLoadedState: ValueState[Boolean] = _
  // 定义一个新的状态变量，用于存储全局站点偏移量
  private var globalSiteOffsetsState: ValueState[Map[String, Int]] = _

  private var folderPath: String = "/opt/data_json" // 文件夹路径
  private var folderPath02: String = "F:\\data" // 目录路径

  override def open(parameters: Configuration): Unit = {
    rulesState = getRuntimeContext.getListState(
      new ListStateDescriptor[Map[String, List[DiagnosisRule]]]("DiagnosisRule", classOf[Map[String, List[DiagnosisRule]]])
    )

    val descriptor = new ListStateDescriptor[InputData](
      "allDataState", // 状态名称
      createTypeInformation[InputData] // 数据类型
    )
    // 获取键控状态
    allDataState = getRuntimeContext.getListState(descriptor)

    // 设置默认值为 false
    val isRulesLoade = new ValueStateDescriptor[Boolean]("isRulesLoaded", classOf[Boolean], false)
    isRulesLoadedState = getRuntimeContext.getState(isRulesLoade)

    // 初始化数据库连接
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"

    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)
    connection.setAutoCommit(true)



    // 准备 SQL
    val fullQuery =s"""
      |SELECT *,
      |  CASE
      |    WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2
      |    THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
      |    ELSE split_part(iot_fld, '_', 1)
      |  END AS iot_dev
      |FROM public.kr_diagnosis_rules
      |WHERE iot_tbl = ?
      |""".stripMargin

    psFull = connection.prepareStatement(fullQuery)
    psFull.setFetchSize(1000)

    val incrementalQuery =s"""
      |SELECT *,
      |  CASE
      |    WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2
      |    THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
      |    ELSE split_part(iot_fld, '_', 1)
      |  END AS iot_dev
      |FROM public.kr_diagnosis_rules
      |WHERE iot_tbl = ? AND updated_at > ?
      |ORDER BY updated_at, id
      |""".stripMargin

    psIncremental = connection.prepareStatement(incrementalQuery)
    psIncremental.setFetchSize(1000)
  }

  def loadRules(): Unit = {
    val folder = new File(folderPath)
    if (folder.exists() && folder.isDirectory) {
      folder.listFiles().foreach { file =>
        if (file.isFile && file.getName.endsWith(".json")) {
          try {
            val jsonStr = scala.io.Source.fromFile(file).mkString
            val jsonData = Json.parse(jsonStr)

            import JsonFormats._
            val ruleWrapper = jsonData.as[RuleWrapper]

            // 处理 input_data 并去重
            val dataCount = ruleWrapper.rule.input_data.distinct

            // 直接将数据添加到状态
            dataCount.foreach(data => allDataState.add(data))

          } catch {
            case ex: Exception =>
              println(s"[Error] Failed to process file ${file.getAbsolutePath}: ${ex.getMessage}")
          }
        }
      }
    }
  }


  // 增量刷新
  def refreshRulesData(key: String): Unit = {
//    val currentRules: List[DiagnosisRule] = rulesCache.value().getOrElse(key, mutable.ArrayBuffer.empty[DiagnosisRule]).toList
    val currentRules = Option(rulesCache.value())
      .map(_.getOrElse(key, mutable.ArrayBuffer.empty[DiagnosisRule]))
      .getOrElse(mutable.ArrayBuffer.empty[DiagnosisRule])
    val validTimestamps: Iterable[Timestamp] = currentRules
      .map(_.updated_at)
      .filter(_ != null)
    // 2. 手动计算最大值（处理空集合情况）
    val maxTime: Option[Timestamp] = if (validTimestamps.nonEmpty) {
      Some(validTimestamps.max)
    } else None

    try {
      // 查询全量数据
      //要截取key再获取数据
      val parts = key.split("_")
      val tableKey = if (parts.length > 1) {
        parts.init.mkString("_") // 所有除最后一个部分外拼接回来
      } else {
        key
      }

      // 2. 将字符串时间转换为 Timestamp
//      val defaultTimeStr = "2025-07-07 08:00:00"
//      val lastUpdateTime = maxTime
//        .map(ts => new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts))
//        .getOrElse(defaultTimeStr)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("GMT+8")) // 设置解析时区

      val defaultTimeStr = "2025-07-07 08:00:00"
      val defaultMillis = sdf.parse(defaultTimeStr).getTime
      val defaultTime = maxTime.getOrElse(new Timestamp(defaultMillis))

      psIncremental.setString(1, tableKey)
      psIncremental.setTimestamp(2, defaultTime)
      //获取数据
      var resultSet: ResultSet = null
      try {
        resultSet = psIncremental.executeQuery()
        if (resultSet != null) {
          while (resultSet.next()) {
            val rule = createDiagnosisRuleFromResultSet(resultSet)
            // 获取当前规则缓存（直接获取 ArrayBuffer）
//            val currentRules = rulesCache.value().getOrElse(partitionKey, mutable.ArrayBuffer.empty[DiagnosisRule])
            val currentRules = Option(rulesCache.value())
              .map(_.getOrElse(key, mutable.ArrayBuffer.empty[DiagnosisRule]))
              .getOrElse(mutable.ArrayBuffer.empty[DiagnosisRule])
            // 添加规则并保持有序（按ID排序）
            if(!currentRules.contains(rule)){
              currentRules += rule
              currentRules.sortBy(_.id)
              rulesCache.update(Map(key -> currentRules))
            }
          }
        }
      }
      // 合并到缓存
    } catch {
      case e: Exception =>
        println(s"查询规则数据出错: ${e.getMessage}")
    }
  }


  private def fetchRules01(fullSql: String, key: String): Unit = {
    var resultSet: ResultSet = null
    try {
      val statement=connection.prepareStatement(fullSql)
      statement.setFetchSize(1000)
      resultSet = statement.executeQuery()

      while (resultSet.next()) {
        val rule = createDiagnosisRuleFromResultSet(resultSet)
        val siteId = s"root.ln.`${rule.site_id}`"

        // 获取当前计数器值，处理null
        val counter = Option(siteCountersState.get(key)).getOrElse(0L)

        // 使用siteId获取分区数
        val numPartitions = sitePartitionMap.getOrElse(siteId, 1)
        val modValue = counter % numPartitions
        val partitionKey = s"${siteId}_${modValue}"

        // 获取当前规则缓存
        val currentRulesMap = Option(rulesCache.value()).getOrElse(Map.empty)

        // 获取或创建当前分区的规则集合
        val currentRules = currentRulesMap.getOrElse(
          partitionKey,
          mutable.ArrayBuffer.empty[DiagnosisRule]
        )

//        logger.info(s"处理siteId=${siteId}，分区=${partitionKey}，当前规则数=${currentRules.size}，计数器=${counter}")

        // 添加规则（如果不存在）
        if (!currentRules.exists(_.id == rule.id)) {
          currentRules += rule
          currentRules.sortBy(_.id)

          // 更新规则缓存
          val updatedRulesMap = currentRulesMap.updated(partitionKey, currentRules)
          rulesCache.update(updatedRulesMap)

          // 更新计数器（使用MapState的put方法）
          siteCountersState.put(key, counter + 1)

          logger.info(s"成功添加规则=${partitionKey}，新规则数=${currentRules.size}，新计数器=${counter + 1}")
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"获取规则数据出错: ${e.getMessage}", e)
    } finally {
      if (resultSet != null) resultSet.close()
    }
  }

  // 辅助方法：为特定设备的站点构建局部偏移量映射
  private def buildDeviceSiteOffsets(deviceSites: List[String]): Map[String, Int] = {
    val deviceSitePartitionMap = sitePartitionMap.filterKeys(deviceSites.contains)
    var offset = 0
    deviceSitePartitionMap.toList.flatMap { case (siteId, numPartitions) =>
      val result = (siteId, offset)
      offset += numPartitions
      List(result)
    }.toMap
  }

  // 在类中添加一个辅助方法，用于统一生成分区键
  private def generatePartitionKey(siteId: String, counter: Long, deviceSites: List[String]): String = {
    // 为当前设备的站点构建局部偏移量映射
    val deviceSiteOffsets = buildDeviceSiteOffsets(deviceSites)

    // 获取站点分区数
    val numPartitions = sitePartitionMap.getOrElse(siteId, 1)

    // 确保计数器不超过分区数
    val adjustedCounter = counter % numPartitions

    // 生成最终分区键
    val offset = deviceSiteOffsets.getOrElse(siteId, 0)
    s"${siteId}_${offset + adjustedCounter}"
  }

  private def fetchRules(fullSql: String, key: String): Unit = {
    var resultSet: ResultSet = null
    try {
      val statement = connection.prepareStatement(fullSql)
      statement.setFetchSize(1000)
      resultSet = statement.executeQuery()

      // 收集当前设备相关的所有站点
      val deviceSites = mutable.Set[String]()
      val tempRules = mutable.ArrayBuffer[DiagnosisRule]()

      // 第一遍扫描：收集站点信息
      while (resultSet.next()) {
        val rule = createDiagnosisRuleFromResultSet(resultSet)
        val siteId = s"root.ln.`${rule.site_id}`"
        deviceSites += siteId
        tempRules += rule
      }

      val siteSet = sitePartitionMap.keySet.toList
      // 转换为不可变List供后续使用
      val deviceSitesList = deviceSites.toList

      // 第二遍处理：应用局部偏移量生成正确的分区键
      for (rule <- tempRules) {
        val siteId = s"root.ln.`${rule.site_id}`"

        // 获取当前计数器值，处理null
        val counter = Option(siteCountersState.get(key)).getOrElse(0L)

        // 使用统一的分区键生成方法
        val partitionKey = generatePartitionKey(siteId, counter, siteSet)

        // 获取当前规则缓存
        val currentRulesMap = Option(rulesCache.value()).getOrElse(Map.empty)

        // 获取或创建当前分区的规则集合
        val currentRules = currentRulesMap.getOrElse(
          partitionKey,
          mutable.ArrayBuffer.empty[DiagnosisRule]
        )

        // 添加规则（如果不存在）
        if (!currentRules.exists(_.id == rule.id)) {
          currentRules += rule
          currentRules.sortBy(_.id)

          // 更新规则缓存
          val updatedRulesMap = currentRulesMap.updated(partitionKey, currentRules)
          rulesCache.update(updatedRulesMap)

          // 更新计数器
          siteCountersState.put(key, counter + 1)

          logger.info(s"成功添加规则=${partitionKey}，新规则数=${currentRules.size}，新计数器=${counter + 1}")
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"获取规则数据出错: ${e.getMessage}", e)
    } finally {
      if (resultSet != null) resultSet.close()
    }
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

  def getSqlQuery(lastTsOpt: Option[Long], iotFlds: String, device: String, lastTsOpt02: Option[Long]): String = {
    val parts = device.split("_")
    val tableKey = if (parts.length > 1) {
      parts.init.mkString("_") // 所有除最后一个部分外拼接回来
    } else {
      device
    }
    if (lastQueryTime.value() == null) {
      lastQueryTime.update(LocalDateTime.now())
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
        s"SELECT ${iotFlds} FROM ${tableKey}  where time>=${timestamp} ORDER BY time asc LIMIT 1 "
    }
  }

  override def processElement(key: (String,String), context: KeyedProcessFunction[String, (String,String), DiagnosisResult]#Context, collector: Collector[DiagnosisResult]): Unit = {
    val isRulesLoaded = isRulesLoadedState.value()
    val parts = key._2.split("_")
    val tableKey = if (parts.length > 1) {
      parts.init.mkString("_") // 所有除最后一个部分外拼接回来
    } else {
      key._2
    }
    if (!isRulesLoaded) {
      psFull.setString(1, tableKey)
      //获取数据
      loadRules()
      val filterData: Iterable[InputData] = allDataState.get()
      val filteredData = filterData.filter(_.position.contains("_valid"))
      // 现在 filteredData 是包含 "_valid" 的 InputData 对象列表

      // 如果需要提取这些对象的 position 字段
      val validPositions = filteredData
        .map { item =>
          val pos = item.position.replaceAll("_valid$", ""
          )
          s"'$pos'" // 每个值加单引号
        }.mkString(",")

      val fuSql = if(validPositions.nonEmpty) psFull+"and iot_fld not in("+validPositions+")" else psFull.toString

      fetchRules(fuSql,key._2)
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

//      val rulesForDevice = rulesCache.value().getOrElse(key._2, mutable.ArrayBuffer.empty[DiagnosisRule])
      val rulesForDevice = Option(rulesCache.value())
        .flatMap(_.get(key._2))
        .getOrElse(mutable.ArrayBuffer.empty[DiagnosisRule])
      //直接查找
      logger.info(s"打印key:${key._2} ${Thread.currentThread().getName} 长度值：${rulesForDevice.size}")
      if(rulesForDevice.nonEmpty){
        val iotFlds = rulesForDevice.map(_.iot_fld).distinct.mkString(",")
//        logger.info(s"分区的key:${key}  总长度：${rulesForDevice.size} ")
        if (iotFlds.nonEmpty) {
          val batchSize = 1700
          val numBatches = math.ceil(rulesForDevice.size.toDouble / batchSize).toInt
          // 根据是否是首次执行决定时间戳
          val (lastTimestampOpt, lastTimestampOpt2) = faStartime.get(key._2) match {
            case Some((t1, t2)) => (t1, t2)
            case None => (None, None)
          }
          for (i <- 0 until numBatches) {
            val startIdx = i * batchSize
            val endIdx = (i + 1) * batchSize.min(rulesForDevice.size)
            val batchRules = rulesForDevice.slice(startIdx, endIdx)
            val iotFldsBatch = batchRules.map(_.iot_fld)
              .filter(_ != null)                             // 过滤 null 的字段
              .filter(_.nonEmpty).mkString(",")
            val device = batchRules.head.iot_tbl // 假设所有规则有相同的设备名

            val sql = getSqlQuery(lastTimestampOpt, iotFldsBatch, device,lastTimestampOpt2)
                        logger.info(s"涉及的key：${key._2} 打印时间:${lastTimestampOpt} ${lastTimestampOpt2}")
            val result = sendRequest(sql)
            val responses = handleResponse(result)
            val timestamps = responses.map(_.timestamp)
            // 获取最大时间戳（Option）
            val maxTimestamp = if (timestamps.nonEmpty) Some(timestamps.max) else None
            if(maxTimestamp.isEmpty||maxTimestamp.get==lastTimestampOpt.getOrElse(0L)){
              val lastTimestampOpts = lastTimestampOpt2.map(_ + 300000)
              facilityStartTime.update(Map(key._2 -> (lastTimestampOpt, lastTimestampOpts)))
            }else{
              facilityStartTime.update(Map(key._2 -> (maxTimestamp,maxTimestamp)))
              responses.foreach(data => {
                val mismatchStart2 = Option(samplingFrequencyMismatchStart.value()).getOrElse(Map.empty[String, (Option[Long], Option[Long])])
                val faultStatesMap2 = Option(faultStates.value()).getOrElse(Map.empty[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)])
                val dedfaultstate2 = Option(dedfaultState.value()).getOrElse(Map.empty[String, List[(Option[Double], Some[Long])]])
                val lastValidValues2 = Option(lastValidValuesState.value()).getOrElse(Map.empty[String, (Option[Any], Option[Long], Option[Double])])
                val diagnosisResult = handleIotDBdata(data, rulesForDevice, lastValidValues2, mismatchStart2, faultStatesMap2, dedfaultstate2)
                collector.collect(diagnosisResult)
              })
            }
          }
        }
      }
    }
  }
  // 重写 onTimer 方法，处理定时触发逻辑
  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, (String,String), DiagnosisResult]#OnTimerContext,
                        out: Collector[DiagnosisResult]
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
}
