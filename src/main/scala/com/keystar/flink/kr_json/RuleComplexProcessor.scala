package com.keystar.flink.kr_json

import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import com.keystar.flink.kr_json.JsonFunction.processResponses
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import java.io.File
import java.lang.Thread
import java.time.Instant
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.mutable


class RuleComplexProcessor(timestamp:Long,sitepartion:mutable.Map[String,Int]) extends KeyedProcessFunction[String, String, OutResult] {

  // 状态变量：保存最近一条数据
  private var lastDataState: ValueState[OutResult] = _

  //设置分区加载数值
  private var isRulesLoadedState: ValueState[Boolean] = _

  private var folderPath01: String = "/opt/data_json01" // 文件夹路径
  private var folderPath: String = "F:\\data01" // 文件夹路径
  //定义分区列表
  private val sitePartitionMap=sitepartion

  //定义计算分区数计数
  private lazy val siteCountersState: MapState[String, Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("siteCounters", classOf[String], classOf[Long])
  )



  //存储需要input点位
  private lazy val rulesInput: MapState[String, Set[InputData]] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Set[InputData]]("rulesInput", classOf[String], classOf[Set[InputData]])
  )

  // 状态变量：保存一段时间内的数据列表
  private var dataListState: ListState[OutResult] = _
  //存储计算表达式
  private var expressionsState: MapState[String, Set[String]] = _
  //存储点位和长度
  private var inputLengthState: MapState[String, Int] = _
  //存储告警需要的点位信息
  private var outMapState: ListState[Map[String, (String, String, Boolean, Boolean, Double, String, String, String, String, String, Map[String, List[Double]], List[Group], List[String])]] = _
  //表达式
  private var scriptsState: ListState[String] = _

  private var continue_data:MapState[String,List[(Any,Long)]]= _

  // 状态管理器，用于记录站点、设备，上次跑的时间
  private lazy val facilityStartTime: ValueState[Map[String, (Option[Long],Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long],Option[Long])]]("facilityStartTime", classOf[Map[String, (Option[Long],Option[Long])]])
  )






  private val logger = LoggerFactory.getLogger(this.getClass)
  // 初始化方法
  override def open(parameters: Configuration): Unit = {
    // 定义 ValueState 描述符（保存最近一条数据）
    val lastDataDesc = new ValueStateDescriptor[OutResult]("lastData", classOf[OutResult])
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.hours(24))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .build()
    lastDataDesc.enableTimeToLive(ttlConfig)

    val outputTableDescriptor = new MapStateDescriptor[String, Set[String]](
      "outputTableState", // 状态名称
      classOf[String],    // 键类型（table 名称）
      classOf[Set[String]] // 值类型（output 列表）
    )
    expressionsState = getRuntimeContext.getMapState(outputTableDescriptor)

    // 初始化 outMapState
    val outMapDescriptor = new ListStateDescriptor[Map[String, (String, String, Boolean, Boolean, Double, String, String, String, String, String, Map[String, List[Double]], List[Group], List[String])]](
      "outMapState", // 状态名称
      classOf[Map[String, (String, String, Boolean, Boolean, Double, String, String, String, String, String, Map[String, List[Double]], List[Group], List[String])]]
    )
    outMapState = getRuntimeContext.getListState(outMapDescriptor)

    val scriptsDescriptor = new ListStateDescriptor[String](
      "scriptsState", // 状态名称
      classOf[String]
    )
    scriptsState = getRuntimeContext.getListState(scriptsDescriptor)

    // 设置默认值为 false
    val isRulesLoade = new ValueStateDescriptor[Boolean]("isRulesLoaded", classOf[Boolean], false)
    isRulesLoadedState = getRuntimeContext.getState(isRulesLoade)

    val descriptor = new MapStateDescriptor[String, List[(Any, Long)]](
      "lastreadIotDB",
      classOf[String],
      classOf[List[(Any, Long)]]
    )

    continue_data= getRuntimeContext.getMapState(descriptor)

    // 初始化 inputLengthState
    val inputLengthDescriptor = new MapStateDescriptor[String, Int](
      "inputLengthState", // 状态名称
      classOf[String],    // 键类型
      classOf[Int]        // 值类型
    )
    inputLengthState = getRuntimeContext.getMapState(inputLengthDescriptor)

  }

  private def loadRules(key:String): Unit = {
    val folder = new File(folderPath)
    if (folder.exists() && folder.isDirectory) {
      folder.listFiles().foreach { file =>
        if (file.isFile && file.getName.endsWith(".json")) {
          try {
            // 读取文件内容并解析为 JSON
            val jsonStr = scala.io.Source.fromFile(file).mkString
            val jsonData = Json.parse(jsonStr)
            val fileName = file.getName
            // 使用 RuleWrapper 解析 JSON 数据
            import JsonFormats._
            val ruleWrapper = jsonData.as[RuleWrapper]

            // 处理 input_length

            val siteId = ruleWrapper.rule.input_data.headOption
              .map(_.table)
              .getOrElse("")



            // 处理 outMap
            val newOutMap = ruleWrapper.rule.output_data.map(data =>
              Map(data.output -> (data.table, data.position, data.alarm,data.duration_status,data.duration_seconds,data.level,data.properties,data.alg_desc,data.alg_label_EN,data.alg_brief_CH,data.alg_param,data.alg_show_group,data.alg_show_levelcursor))
            ).toList


            val outputs: Set[String] = ruleWrapper.rule.output_data.map(_.output).toSet
            val inputs: Set[InputData] = ruleWrapper.rule.input_data.toSet

            if(key==siteId){
//              println("key和站点名称相同111")
              if (expressionsState.contains(siteId)) {
                val existingInputDataSet = expressionsState.get(siteId)
                val updatedInputDataSet = existingInputDataSet ++ outputs
                // 将文件存储至状态管理器里边
                expressionsState.put(siteId, updatedInputDataSet)
              } else {
                expressionsState.put(siteId, outputs)
              }

              if (rulesInput.contains(siteId)) {
                val existingInputDataSet = rulesInput.get(siteId)
                val updatedInputDataSet = existingInputDataSet ++ inputs
                // 将文件存储至状态管理器里边
                rulesInput.put(siteId, updatedInputDataSet)
              } else {
                rulesInput.put(siteId, inputs)
              }
            }


            // 将新数据添加到 outMapState
            newOutMap.foreach(outMapState.add)

            // 处理 scripts
            ruleWrapper.rule.script.foreach(scriptsState.add)

          } catch {
            case ex: Exception =>
              println(s"[Error] Failed to process file ${file.getAbsolutePath}: ${ex.getMessage}")
          }
        }
      }

//      val ruleSize = rulesInput.get(key).size
      val ruleSize = Option(rulesInput.get(key)).map(_.size).getOrElse(0)
//      println(s"打印input的长度：${ruleSize},${key}")
      inputLengthState.put(key,ruleSize)
    }
  }

  // 核心处理逻辑
  override def processElement(
                               value: String,
                               ctx: KeyedProcessFunction[String, String, OutResult]#Context,
                               out: Collector[OutResult]
                             ): Unit = {
    // 1. 加载分区文件数据
    val isRulesLoaded = isRulesLoadedState.value()
    val key = value.dropRight(2)
    if (!isRulesLoaded) {
      loadRules(key)
      isRulesLoadedState.update(true)
    }
    //2.获取对应的表达式和数据
    val filter_data =rulesInput.get(key)
    //设备时间
    val faStartime = Option(facilityStartTime.value()).getOrElse(Map.empty[String, (Some[Long],Some[Long])])

    // 判断是否为首次执行
    val isFirstExecution = faStartime.isEmpty || !faStartime.contains(key)

    val matchingExpressions = expressionsState.get(key).toSeq
    //3.表达式计算
    //      println(s"打印这个数值：${matchingExpressions.size}")
    val sqlQuery: Seq[Seq[(String, String, Any, Long)]] = buildSqlQuery(filter_data,matchingExpressions, faStartime,isFirstExecution,key)
    //4.往下游输出
    def processSiteData(sqldata: (String, String, Any, Long), outMap: List[Map[String, (String, String, Boolean, Boolean, Double, String, String, String, String, String, Map[String, List[Double]], List[Group], List[String])]]): Unit = {
      val (site, point, collectedValue, timestamp) = sqldata

      // 将 collectedValue 转换为布尔值
      val valueAsBoolean = collectedValue match {
        case bool: Boolean => bool
        case num: Number =>
          val numValue = num.intValue()
          numValue == 1 || numValue == 2
        case _ => false
      }

      //        val duration_status=true
      //        val duration_seconds=30
      // 查找与站点相关的映射
      outMap.find(_.contains(site)).foreach { siteMap =>
        siteMap.get(site).foreach { case (station,expression,alarm,duration_status,duration_seconds,level,properties,alg_desc,alg_label_EN,alg_brief_CH,alg_param,alg_show_group,alg_show_levelcursor) =>
          val exp_key=s"$station-$expression"

          // 清空状态管理器的逻辑单独处理
          if (!valueAsBoolean && continue_data.contains(exp_key)) {
            // 如果 collectedValue 为 false，则清空状态管理器中的数据
            continue_data.remove(exp_key)
          }

          // 检测 duration_status 是否为 true 且数值是否为布尔值
          if (duration_status && valueAsBoolean&& continue_data.contains(exp_key)) {
            // 获取当前状态管理器中的数据点
            val currentState = continue_data.get(exp_key)
            val updatedState = currentState.filter { case (_, t) =>
              timestamp - t < duration_seconds * 1000 // 只保留最近 duration_seconds 秒内的数据
            } :+ (collectedValue -> timestamp)

            // 检查数据点之间的间隔是否符合要求（每 15 秒一个数据点）
            val validIntervals = updatedState.sliding(2).forall { case List((_, t1), (_, t2)) =>
              (t2 - t1) == 15 * 1000
            }

            if (validIntervals) {
              // 计算时间跨度
              val timeSpan = updatedState.last._2 - updatedState.head._2

              if (timeSpan >= duration_seconds * 1000) {
                // 如果时间跨度达到或超过 duration_seconds 秒，则检查所有数据点是否一致
                val allValuesValid = updatedState.map(_._1).distinct.size == 1
                if (allValuesValid) {
                  // 触发告警（满足条件就告警）
                  out.collect(OutResult(
                    timestamp = timestamp,
                    expressions = expression,
                    collectedValue = Some(true),
                    alarm = alarm,
                    level=level,
                    station = station,
                    properties=properties,
                    alg_desc=alg_desc,
                    alg_label_EN=alg_label_EN,
                    alg_brief_CH=alg_brief_CH,
                    alg_param=alg_param,
                    alg_show_group=alg_show_group,
                    alg_show_levelcursor=alg_show_levelcursor
                  ))
                }
              }
            } else {
              // 数据点间隔不符合要求，清空状态管理器
              continue_data.remove(exp_key)
              if(valueAsBoolean){
                out.collect(OutResult(
                  timestamp = timestamp,
                  expressions = expression,
                  collectedValue = Some(collectedValue),
                  alarm = alarm,
                  level=level,
                  station = station,
                  properties=properties,
                  alg_desc=alg_desc,
                  alg_label_EN=alg_label_EN,
                  alg_brief_CH=alg_brief_CH,
                  alg_param=alg_param,
                  alg_show_group=alg_show_group,
                  alg_show_levelcursor=alg_show_levelcursor
                ))
              }
            }
          }else{
            //往下游输出
            out.collect(OutResult(
              timestamp = timestamp,
              expressions = expression,
              collectedValue = Some(collectedValue),
              alarm=alarm,
              level=level,
              station = station,
              properties=properties,
              alg_desc=alg_desc,
              alg_label_EN=alg_label_EN,
              alg_brief_CH=alg_brief_CH,
              alg_param=alg_param,
              alg_show_group=alg_show_group,
              alg_show_levelcursor=alg_show_levelcursor
            ))
          }
        }
      }
    }
    val num = outMapState.get().toList
    // 在主逻辑中调用
    val sortedSqlQuery = sqlQuery.map { innerSeq =>
      innerSeq.sortBy(_._4) // 按元组的第4个元素(Long)排序
    }

    // 然后处理排序后的数据
    sortedSqlQuery.foreach { sqldata =>
      sqldata.foreach(processSiteData(_, num))
    }
  }

  private def buildSqlQuery(
                             data: Iterable[InputData],
                             expressions: Seq[String],
                             faStartime: Map[String, (Option[Long],Option[Long])],
                             isFirstExecution: Boolean,
                             input_table:String
                           ): Seq[Seq[(String, String, Any, Long)]] = {

    // 不再按 site 分组，直接处理所有输入数据
    val positions = data.map(_.position).mkString(",")


    // 分离有默认值和无默认值的输入数据
    val (defaultInputs, nonDefaultInputs) = data.partition(_.value.nonEmpty)

    // 将有默认值的数据转换为 Map
    val defaultValueMap: Map[String, String] = defaultInputs.map(input => (input.table + "." + input.position -> input.value)).toMap


    // 根据是否是首次执行决定时间戳
    val (lastTimestampOpt, lastTimestampOpt2) = if (isFirstExecution) {
      (None, None)
    } else {
      faStartime.values.headOption match {
        case Some((opt1, opt2)) => (opt1, opt2)
        case None => (None, None)
      }
    }
    // 正常处理
    val iotSql  = getSqlQuery(lastTimestampOpt, positions, input_table,lastTimestampOpt2)
//    println(s"打印数据22上sql:,$iotSql,时间：$lastTimestampOpt")
    val result = sendRequest(iotSql)
    val responses: List[IoTDBReading] = handleResponse(result)
    if (responses.isEmpty) {
        // 如果结果为空，强制更新时间戳
        val lastTimestampOpts = lastTimestampOpt2.map(_ + 300000) // 增加 300 秒（5 分钟）
        facilityStartTime.update(Map(input_table -> (lastTimestampOpt,lastTimestampOpts)))
    }
    //更新时间戳
    updateFacilityStartTime(input_table, responses)

      // 替换数据库查询结果中的值
    val combinedResponses = responses
//    println(s"打印key:${input_table}")
    val processedResults = processResponses(combinedResponses, data, expressions)

    // 将处理后的结果与时间戳组合
    val finalResults = processedResults.map { data =>
      (data._1, data._2, if (data._3 == None) null else data._3.get, data._4)
    }
    // 将 finalResults 用一个序列包裹起来，以满足返回类型要求
    Seq(finalResults)
  }


  private def updateFacilityStartTime(site: String, responses: List[IoTDBReading]): Option[Long] = {
    if (responses.nonEmpty) {
      val maxTimestamp = responses.map(_.timestamp).max
      facilityStartTime.update(Map(site -> (Some(maxTimestamp),Some(maxTimestamp))))
      Some(maxTimestamp)
    } else {
      None
    }
  }

  def getSqlQuery(lastTsOpt: Option[Long], iotFlds: String, device: String, lastTsOpt02: Option[Long]): String = {
    val parts = device.split("_")
    val tableKey = if (parts.length > 1) {
      parts.init.mkString("_") // 所有除最后一个部分外拼接回来
    } else {
      device
    }
    //    println(s"进来的点位主键key:$tableKey")
    (lastTsOpt, lastTsOpt02) match {
      case (Some(lastTs), Some(lastTs2)) if lastTs == lastTs2 =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(0).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(450).toEpochMilli
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case (Some(lastTs), Some(lastTs2)) if lastTs < lastTs2 =>
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time > ${lastTs} AND time < ${lastTs2}"
      case (Some(lastTs), _) =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(0).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(450).toEpochMilli
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case _ =>
        s"SELECT ${iotFlds} FROM ${tableKey}  where time>=${timestamp} ORDER BY time asc LIMIT 30 "
    }
  }

}