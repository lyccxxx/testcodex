package com.keystar.flink.kr_json


import com.keystar.flink.iotdbfunction.DiagnosisResult
import com.keystar.flink.iotdbfunction.IotdbFunction.Diagnostic_method
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import io.circe.parser.parse
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import play.api.libs.json.Json

import java.io.File
import java.sql.{Connection, DriverManager}
import java.time.Instant
import java.util.function.Function
import java.util.{List => JavaList}
import javax.script.ScriptEngineManager
import scala.collection.JavaConverters.{asScalaBufferConverter, iterableAsScalaIterableConverter}
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex
import org.slf4j.LoggerFactory

import scala.collection.mutable


class RuleProcessor(outputTag: OutputTag[DiagnosisResult],timestamp:Long,sitepartion:mutable.Map[String,Int]) extends CoProcessFunction[String, (String, InputData), OutResult] {
  //设置一个状态管理器,用于存储传入的业务input数据
  private lazy val StateInputData: ListState[InputData] = getRuntimeContext.getListState(
    new ListStateDescriptor[InputData]("resultState", classOf[InputData]))

  // 状态管理器，用于记录站点、设备，上次跑的时间
  private lazy val facilityStartTime: ValueState[Map[String, (Option[Long],Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long],Option[Long])]]("facilityStartTime", classOf[Map[String, (Option[Long],Option[Long])]])
  )
  // 状态管理器，用于存储每个测点的上一个有效值及其时间戳
  private lazy val lastValidValuesState: ValueState[Map[String, (Option[Any], Option[Long], Option[Double])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Any], Option[Long], Option[Double])]]("lastValidValues", classOf[Map[String, (Option[Any], Option[Long], Option[Double])]])
  )

  // 状态管理器，用于记录采样频率不符问题开始的时间戳
  private lazy val samplingFrequencyMismatchStart: ValueState[Map[String, (Option[Long], Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long])]]("samplingFrequencyMismatchStart", classOf[Map[String, (Option[Long], Option[Long])]])
  )


  // 状态管理器，用于记录每个传感器的死值故障状态
  private lazy val dedfaultState: ValueState[Map[String, List[(Option[Double], Some[Long])]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, List[(Option[Double], Some[Long])]]]("dedfaultState", classOf[Map[String, List[(Option[Double], Some[Long])]]])
  )

  // 状态管理器，用于记录故障状态
  private lazy val faultStates: ValueState[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]]("faultStates", classOf[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]])
  )

  //定义一个记录规则数据是否加载完成的标识
  // 使用 MapState 存储 Map[String, List[InputData]] 类型的数据
  private lazy val rulesState: MapState[String, Set[InputData]] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Set[InputData]]("InputData", classOf[String], classOf[Set[InputData]])
  )


  private lazy val rulesInput: MapState[String, mutable.Set[InputData]] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, mutable.Set[InputData]]("rulesInput", classOf[String], classOf[mutable.Set[InputData]])
  )



  private var connection: Connection = _

  private lazy val siteCountersState: MapState[String, Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("siteCounters", classOf[String], classOf[Long])
  )

  //定义分区列表
  private val sitePartitionMap=sitepartion


  private var folderPath02: String = "/opt/data_json" // 文件夹路径
  private var folderPath: String = "F:\\data" // 目录路径
  // 定义状态变量
  private var inputLengthState: MapState[String, Int] = _
  private var outMapState: ListState[Map[String, (String, String, Boolean, Boolean, Double, String, String, String, String, String, Map[String, List[Double]], List[Group], List[String])]] = _
  private var expressionsState: MapState[String, Set[String]] = _
  private var scriptsState: ListState[String] = _
  //设置分区加载数值
  private var isRulesLoadedState: ValueState[Boolean] = _
  private var lastreadIotDB: MapState[String, List[(Any, Long)]] = _
  private var continue_data:MapState[String,List[(Any,Long)]]= _

  private var rule_data:ListBuffer[DiagnosisRule]= _

  private val logger = LoggerFactory.getLogger(this.getClass)
  override def open(parameters: Configuration): Unit = {
    // 初始化 inputLengthState
    val inputLengthDescriptor = new MapStateDescriptor[String, Int](
      "inputLengthState", // 状态名称
      classOf[String],    // 键类型
      classOf[Int]        // 值类型
    )
    inputLengthState = getRuntimeContext.getMapState(inputLengthDescriptor)


    // 初始化 outMapState
    val outMapDescriptor = new ListStateDescriptor[Map[String, (String, String, Boolean, Boolean, Double, String, String, String, String, String, Map[String, List[Double]], List[Group], List[String])]](
      "outMapState", // 状态名称
      classOf[Map[String, (String, String, Boolean, Boolean, Double, String, String, String, String, String, Map[String, List[Double]], List[Group], List[String])]]
    )
    outMapState = getRuntimeContext.getListState(outMapDescriptor)


    // 初始化 expressionsState
    val outputTableDescriptor = new MapStateDescriptor[String, Set[String]](
      "outputTableState", // 状态名称
      classOf[String],    // 键类型（table 名称）
      classOf[Set[String]] // 值类型（output 列表）
    )
    expressionsState = getRuntimeContext.getMapState(outputTableDescriptor)
//    println(s"expressionsState: ${expressionsState}") // 添加日志语句

    val descriptor = new MapStateDescriptor[String, List[(Any, Long)]](
      "lastreadIotDB",
      classOf[String],
      classOf[List[(Any, Long)]]
    )
    lastreadIotDB = getRuntimeContext.getMapState(descriptor)

    continue_data= getRuntimeContext.getMapState(descriptor)
    // 初始化 scriptsState
    val scriptsDescriptor = new ListStateDescriptor[String](
      "scriptsState", // 状态名称
      classOf[String]
    )
    scriptsState = getRuntimeContext.getListState(scriptsDescriptor)


    // 设置默认值为 false
    val isRulesLoade = new ValueStateDescriptor[Boolean]("isRulesLoaded", classOf[Boolean], false)
    isRulesLoadedState = getRuntimeContext.getState(isRulesLoade)
  }

  private def loadRules(key:String): Unit = {
    val folder = new File(folderPath)
    val siteSet = sitePartitionMap.keySet.toList
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


            val siteId = ruleWrapper.rule.input_data.headOption
              .map(_.table)
              .getOrElse("")



            val dataCount = ruleWrapper.rule.input_data.distinct.size

            val numPartitions = sitePartitionMap.getOrElse(siteId, 1)


            // 获取当前计数器值
            val currentCounter = siteCountersState.get(siteId)

            // 计算分区键（现在所有并行任务共享计数器）
            val modValue = currentCounter % numPartitions
            val partitionKey = generatePartitionKey(siteId, currentCounter, siteSet)

            println(s"打印文件名和长度22 key：${fileName} ${ruleWrapper.rule.input_data.size} ${partitionKey}")

            ruleWrapper.rule.input_data.foreach{data=>
              val newInputDataSet = mutable.Set(data)

              if (rulesInput.contains(partitionKey)) {
                val existingInputDataSet = rulesInput.get(partitionKey)
                val updatedInputDataSet = existingInputDataSet ++ newInputDataSet
                // 将文件存储至状态管理器里边
                rulesInput.put(partitionKey, updatedInputDataSet)
              } else {
                rulesInput.put(partitionKey, newInputDataSet)
              }
            }
            // 更新计数状态
            val currentLength = Option(inputLengthState.get(partitionKey)).getOrElse(0)


            siteCountersState.put(siteId,currentCounter+1)

            // 处理 outMap
            val newOutMap = ruleWrapper.rule.output_data.map(data =>
              Map(data.output -> (data.table, data.position, data.alarm,data.duration_status,data.duration_seconds,data.level,data.properties,data.alg_desc,data.alg_label_EN,data.alg_brief_CH,data.alg_param,data.alg_show_group,data.alg_show_levelcursor))
            ).toList


            val outputs: Set[String] = ruleWrapper.rule.output_data.map(_.output).toSet

            if(key==partitionKey){
              if (expressionsState.contains(partitionKey)) {
                val existingInputDataSet = expressionsState.get(partitionKey)
                val updatedInputDataSet = existingInputDataSet ++ outputs
                // 将文件存储至状态管理器里边
                expressionsState.put(partitionKey, updatedInputDataSet)
              } else {
                expressionsState.put(partitionKey, outputs)
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
      val ruleSize = rulesInput.get(key).size
      inputLengthState.put(key,ruleSize)
    }
  }

  private def buildSqlQuery(
                             data: Iterable[InputData],
                             expressions: Seq[String],
                             scripts: Seq[String], // 这个参数在原代码中没有被使用，考虑是否需要保留
                             faStartime: Map[String, (Option[Long],Option[Long])],
                             isFirstExecution: Boolean,
                             context: CoProcessFunction[String, (String, InputData), OutResult]#Context,
                             input_table:String
                           ): Seq[Seq[(String, String, Any, Long)]] = {

    // 不再按 site 分组，直接处理所有输入数据
    val positions = data.map(_.position).mkString(",")

    // 检查是否存在复杂值，要是存在进行单测点测试

    val singlefile = positions.split(",").filter(iot_fld => iot_fld.contains("_valid") || iot_fld.endsWith("_basicerr")).distinct.mkString(",")

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
//    println(s"上一个执行打印时间：$lastTimestampOpt")

    val processedResults = if (singlefile.nonEmpty) {
      handleFaultyInputs(input_table, data, expressions, lastTimestampOpt, defaultValueMap,context,lastTimestampOpt2)
    } else {
      // 正常处理
      val iotSql  = getSqlQuery(lastTimestampOpt, positions, input_table,lastTimestampOpt2)
//      println(s"打印数据22上sql:,$input_table,时间：$lastTimestampOpt")
      val result = sendRequest(iotSql)
      val responses: List[IoTDBReading] = handleResponse(result)
//      val maxTimestamp = responses.map(_.timestamp).max
//      logger.info(s"无valid值处理的长度：${responses.size} 主键：${input_table}")
      val timestamps = responses.map(_.timestamp)
      // 获取最大时间戳（Option）
      var processSeq: Seq[(String, String, Option[Any], Long)]=mutable.Seq.empty
      val maxTimestamp = if (timestamps.nonEmpty) Some(timestamps.max) else None
      if (responses.isEmpty || maxTimestamp.getOrElse(0L)==lastTimestampOpt.getOrElse(0L)) {
        // 如果结果为空，强制更新时间戳
        val lastTimestampOpts = lastTimestampOpt2.map(_ + 300000) // 增加 300 秒（5 分钟）
        facilityStartTime.update(Map(input_table -> (lastTimestampOpt,lastTimestampOpts)))
      }else{
        val updatedFaStartTime = updateFacilityStartTime(input_table, responses,lastTimestampOpt)
        // 替换数据库查询结果中的值
        val combinedResponses = responses
        logger.info(s"开始进入无vaild打印key:${input_table}")
        processSeq= processResponses(combinedResponses, data, expressions, input_table)
      }
      processSeq
    }

    val faStartime_new = Option(facilityStartTime.value()).getOrElse(Map.empty[String, Some[Long]])

    // 将处理后的结果与时间戳组合
    val finalResults = processedResults.map { data =>
      (data._1, data._2, if (data._3 == None) null else data._3.get, data._4)
    }

    // 将 finalResults 用一个序列包裹起来，以满足返回类型要求
    Seq(finalResults)
  }

  // 辅助函数：用默认值替换查询结果中的相应位置的数据
  def replaceWithDefaults(values: Map[String, Option[Any]], defaultValueMap: Map[String, String]): Map[String, Option[Any]] = {
    values.map { case (key, value) =>
      val defaultValue=defaultValueMap.get(key)
      if(defaultValue.nonEmpty){
        (key, defaultValue)
      }else (key, value)
    }
  }

  import scala.collection.mutable
  def replaceKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): String = {
    inputMap.keys.filter(expr.contains).foldLeft(expr) { (e, key) =>
      inputMap(key) match {
        case Some((currentValue: Any, currentTimestamp: Long)) =>
          val functionCalls = extractFunctionCallsFromExpr(e).filter(_.contains(key))
          if(functionCalls.isEmpty){
            // 使用正则表达式确保只替换独立的单词
            val regex = s"\\b$key\\b".r
            regex.replaceAllIn(e, currentValue.toString)
          }else{
            val newExpr = functionCalls.foldLeft(e) { (innerExpr, functionCall) =>
              val historyValues = {
                val values = lastreadIotDB.get(functionCall)
                if (values != null) {
                  values.filter { case (_, timestamp) => Math.abs(currentTimestamp - timestamp) == 15000 }
                    .map(_._1)
                } else {
                  List.empty
                }
              }
              // 组合历史值和当前值
              val allValues = historyValues :+ currentValue
              // 将函数调用中的参数替换为实际值列表
              val paramStart = functionCall.indexOf('(') + 1
              val paramEnd = functionCall.indexOf(')')
              val param = functionCall.substring(paramStart, paramEnd)
              innerExpr.replace(param, allValues.mkString("[", ",", "]"))
            }

            // 更新 numMap
            functionCalls.foreach { functionCall =>
              if (!lastreadIotDB.contains(functionCall)) {
                // 首次出现，将 currentValue 和 currentTimestamp 存入 numMap
                lastreadIotDB.put(functionCall, List((currentValue, currentTimestamp)))
              } else {
                // 不是首次出现，更新 numMap 中的值
                val updatedValues = List((currentValue, currentTimestamp))
                lastreadIotDB.remove(functionCall)
                lastreadIotDB.put(functionCall, updatedValues)
              }
            }
            newExpr
          }
        case _ => e
      }
    }
  }



  // 更新设施开始时间
  private def updateFacilityStartTime(site: String, responses: List[IoTDBReading],lastTimestampOpt:Option[Long]): Option[Long] = {
    if (responses.nonEmpty) {
      val maxTimestamp = responses.map(_.timestamp).max
      if(maxTimestamp>lastTimestampOpt.getOrElse(0L)){
        facilityStartTime.update(Map(site -> (Some(maxTimestamp),Some(maxTimestamp))))
        Some(maxTimestamp)
      }else lastTimestampOpt
    } else {
      None
    }
  }

  // 处理有故障的输入数据
  private def handleFaultyInputs(site: String,
                                 inputs: Iterable[InputData],
                                 expressions: Seq[String],
                                 lastTimestampOpt: Option[Long],
                                 defaultValueMap:Map[String,String],
                                 context: CoProcessFunction[String, (String, InputData), OutResult]#Context,
                                 lastTsOpt2: Option[Long] ): Seq[(String, String, Option[Any],Long)] = {


    val filterData = inputs.filter { input =>
      val position = input.position
      position.contains("_valid") || position.endsWith("_basicerr")
    }

    if (filterData.isEmpty) return Seq.empty

    //获取到有效值字段
    val singedata = filterData.map { data =>
      // 使用正则表达式替换掉 "_dataphysics" 或 "_datasample" 后的部分
      val regex = "_valid|_basicerr".r
      val result = regex.split(data.position)
      if (result.nonEmpty) result.head else ""
    }.toSet.mkString(",")

    val numdata=filterData.map(data=>data.position
    ).toSet.mkString(",")


    //获取单点规则
    val rulesForDevice: ListBuffer[DiagnosisRule] = filterRules(rule_data,singedata, site)
    val pgiotfld = rulesForDevice.map(data=>data.iot_fld).distinct.mkString(",")
//    logger.info(s"valid值计算01打印从pg里边取的字段:${pgiotfld.size}  ${rulesForDevice.size} iot字段：${singedata.size}")
    if (rulesForDevice.isEmpty) return Seq.empty

    val postData = inputs.filter { input =>
      val position = input.position
      !(position.contains("_valid") || position.endsWith("_basicerr"))
    }

    val postresponses = if(postData.nonEmpty){
      val postfile = postData.map(data=>data.position).toSet.mkString(",")
      val postsql = getSqlQuery(lastTimestampOpt, postfile, site,lastTsOpt2)
//      println(s"valid值计算02打印获取到的ssql: ${postsql} ${lastTimestampOpt},${lastTsOpt2}")
      val postresult = sendRequest(postsql)
      handleResponse(postresult)
      }else List.empty[IoTDBReading]


    val iotSql2 = getSqlQuery(lastTimestampOpt, pgiotfld, site,lastTsOpt2)
//    logger.info(s"0333打印需要校验的规则字段数据：${iotSql2}")
    val result2 = sendRequest(iotSql2)
    val responses02 = handleResponse(result2)

    val combinedResponses3 =if(lastTimestampOpt.isEmpty){
      val reponse03= responses02.map(response => updateResponse(response, rulesForDevice,context))
      val repose03V =replaceKeysAndValues(reponse03,defaultValueMap)
      repose03V
    }else{
      val reponse03= responses02.map(response => updateResponse(response, rulesForDevice,context))
      replaceKeysAndValues(reponse03,defaultValueMap)
    }

    val updatedResponses = if(combinedResponses3.isEmpty) {
      val reponse03= responses02.map(response => updateResponse(response, rulesForDevice,context))
      val rpos3 =replaceKeysAndValues(reponse03,defaultValueMap)
      rpos3
    } else combinedResponses3

    val newResponses = postresponses ++ updatedResponses

    logger.info(s"存在valid值处理的长度：${newResponses.size} 主键：${site} ")
//    val maxTimestamp = newResponses.map(_.timestamp).max
    val timestamps = newResponses.map(_.timestamp)
    var haprocessSeq: Seq[(String, String, Option[Any], Long)]=mutable.Seq.empty
    // 获取最大时间戳（Option）
    val maxTimestamp = if (timestamps.nonEmpty) Some(timestamps.max) else None
    //更新时间
    if (newResponses.isEmpty || maxTimestamp.getOrElse(0L)==lastTimestampOpt.getOrElse(0L)) {
      // 如果结果为空，强制更新时间戳
      val lastTimestampOpts = lastTsOpt2.map(_ + 300000) // 增加 300 秒（5 分钟）
      facilityStartTime.update(Map(site -> (lastTimestampOpt,lastTimestampOpts)))
    }else{
      val updatedFaStartTime = updateFacilityStartTime(site, newResponses,lastTimestampOpt)
      haprocessSeq=processResponses(newResponses, inputs, expressions,site)
    }
    haprocessSeq
  }

  // 定义一个函数来替换键和值
  def replaceKeysAndValues(readings: List[IoTDBReading], keyMapping: Map[String, String]): List[IoTDBReading] = {
    readings.map { reading =>
      val updatedValues = reading.values.flatMap { case (oldKey, value) =>
        // 根据旧键生成新键
        val newKeyPrefix = oldKey.split('.').last
        val newKey = s"${oldKey.replace(s".${newKeyPrefix}", "")}.${newKeyPrefix}_valid"

        // 查找是否有对应的默认值
        val newValue = value.orElse(Some(keyMapping.getOrElse(newKey, 0)))

        Map(newKey -> value)
      }.toMap
      reading.copy(values = updatedValues)
    }
  }

  // 辅助方法：为特定设备的站点构建局部偏移量映射
  def buildDeviceSiteOffsets(deviceSites: List[String]): Map[String, Int] = {
    val deviceSitePartitionMap = sitePartitionMap.filterKeys(deviceSites.contains)
    var offset = 0
    deviceSitePartitionMap.toList.flatMap { case (siteId, numPartitions) =>
      val result = (siteId, offset)
      offset += numPartitions
      List(result)
    }.toMap
  }

  // 在类中添加一个辅助方法，用于统一生成分区键
  def generatePartitionKey(siteId: String, counter: Long, deviceSites: List[String]): String = {
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


  import java.text.DecimalFormat

  private def convertValue(value: Option[Any], valueType: String, accuracy: String): Option[String] = {
    value.flatMap {
      case num: Double =>
        formatNumber(num, accuracy)

      case num: Float =>
        formatNumber(num.toDouble, accuracy) // 将 Float 转换为 Double 进行格式化

      case num: Int =>
        Some(num.toString) // 整数不需要格式化

      case num: Long =>
        Some(num.toString) // 长整型也不需要格式化

      case str: String =>
        try {
          if (str.contains(".")) {
            // 如果字符串包含小数点，尝试解析为 Double
            val parsedNum = str.toDouble
            formatNumber(parsedNum, accuracy)
          } else {
            // 否则尝试解析为 Int
            val parsedNum = str.toInt
            Some(parsedNum.toString)
          }
        } catch {
          case _: NumberFormatException => None // 如果无法解析为数值，返回 None
        }
      case num:Boolean=> Some(num.toString)

      case _ => None // 不支持的类型返回 None
    }
  }

  private def formatNumber(num: Double, accuracy: String): Option[String] = {
    try {
      val precision = accuracy.toInt
      if (precision < 0) {
        Some(num.toString) // 如果精度为负数，直接返回原始值
      } else {
        val pattern = s"#.${"#" * precision}" // 生成格式化模式
        val formatter = new DecimalFormat(pattern)
        Some(formatter.format(num)) // 格式化并返回
      }
    } catch {
      case _: NumberFormatException => Some(num.toString) // 如果精度无效，返回原始值
    }
  }
  // 辅助方法：检查表达式中引用的键是否有空值
  private def hasNullKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): Boolean = {
    inputMap.keys.filter(expr.contains).exists(key => inputMap(key).isEmpty)
  }

  // 提取表达式中的函数调用子表达式
  def extractFunctionCallsFromExpr(expr: String): List[String] = {
    val pattern: Regex = """isStrictly(?:Increasing|Decreasing|Constant)\([^)]+\)""".r
    pattern.findAllMatchIn(expr).map(_.matched).toList.distinct
  }

  // 辅助方法：替换表达式中的键
  def replaceKeysInExpr02(expr: String, inputMap: Map[String, Option[Any]]): String = {
    inputMap.keys.filter(expr.contains).foldLeft(expr) { (e, key) =>
      inputMap(key) match {
        case Some(value: List[Option[Any]]) =>
          // 处理 List[Option[Any]] 类型
          val listStr = value.flatten.map(_.toString).mkString("[", ",", "]")
          e.replace(key, listStr)
        case Some(value: List[_]) =>
          // 处理普通 List 类型
          e.replace(key, value.map(_.toString).mkString("[", ",", "]"))
        case Some(value) if value != null =>
          // 处理其他非 null 值类型
          e.replace(key, value.toString)
        case _ =>
          // 处理 None 或 null 值
          e
      }
    }
  }

  def processResponses(
                        responses: List[IoTDBReading],
                        inputs: Iterable[InputData],
                        expressions: Seq[String],
                        key:String
                      ): Seq[(String, String, Option[Any], Long)] = {

    val engine = new ScriptEngineManager().getEngineByName("JavaScript")

    // 按时间戳分组
    val groupedResponses = responses.groupBy(_.timestamp)
    val groupdata= groupedResponses.flatMap { case (timestamp, readings) =>
      // 合并相同时间戳下的 values
      val mergedValues = readings.flatMap(_.values).toMap



      // 构建输入映射
      val inputMap = inputs.flatMap { data =>
        val attr = data.attr
        val keyToMatch = s"${data.table}.${data.position}"
        val value: Option[Any] = mergedValues.get(keyToMatch).flatMap(identity)
        val convertedValue = convertValue(value, data.`type`, data.accuracy)
        convertedValue.map { cv =>
          // 创建包含转换值和时间戳的元组
          attr -> Some((cv, timestamp))
        }
      }.toMap

      // 先提取所有唯一的键（假设values是Map[String, Any]类型）
      val allKeys = inputMap.keys.toSet

      // 筛选包含任意键的expressions
      val num_express = expressions.filter { expr =>
        allKeys.exists(expr.contains)
      }

      val nsize= num_express.size
      val exsize= expressions.size
      val allsize=allKeys.size
//      println(allsize)
//      println(nsize)
//      println(exsize)


      expressions.map { expr =>
        //先判定这个expr表达式是否存在那三个函数的
        //存在函数的话就获取到上一个状态值，当上一个状态值
        val num03 =hasNullKeysInExpr(expr, inputMap)
        if (hasNullKeysInExpr(expr, inputMap)) {
          (expr, expr, None, timestamp) // 如果表达式中存在空键，直接返回 None
        } else {
          val replacedExpr = replaceKeysInExpr(expr, inputMap)
          try {

            // 注册单调递增
            engine.put("isStrictlyIncreasing", new Function[JavaList[Double], Boolean] {
              override def apply(arrObj: JavaList[Double]): Boolean = {
                val arr: Array[Double] = arrObj.asScala.map(_.doubleValue()).toArray
                isStrictlyIncreasing(arr)
              }
            })

            // 注册单调递减
            engine.put("isStrictlyDecreasing", new Function[JavaList[Double], Boolean] {
              override def apply(arrObj: JavaList[Double]): Boolean = {
                val arr: Array[Double] = arrObj.asScala.map(_.doubleValue()).toArray
                isStrictlyDecreasing(arr)
              }
            })

            // 注册恒定不变
            engine.put("isStrictlyConstant", new Function[JavaList[Double], Boolean] {
              override def apply(arrObj: JavaList[Double]): Boolean = {
                val arr: Array[Double] = arrObj.asScala.map(_.doubleValue()).toArray
                isStrictlyConstant(arr)
              }
            })

            // 动态判断返回值类型
            val result = engine.eval(replacedExpr)
            result match {
              case boolResult if boolResult.isInstanceOf[Boolean] =>
                (expr, replacedExpr, Some(boolResult.asInstanceOf[Boolean]), timestamp)
              case numResult if numResult.isInstanceOf[Double] =>
                (expr, replacedExpr, Some(numResult.asInstanceOf[Double]), timestamp)
              case intResult if intResult.isInstanceOf[Integer] =>
                (expr, replacedExpr, Some(intResult.asInstanceOf[Integer].toDouble), timestamp)
              case strResult if strResult.isInstanceOf[String] =>
                (expr, replacedExpr, Some(strResult.asInstanceOf[String]), timestamp) // 处理 String 类型
              case other =>
                println(s"Unsupported result type for expression '$expr': ${other.getClass}")
                (expr, replacedExpr, None, timestamp) // 不支持的类型返回 None
            }
          } catch {
            case ex: Exception =>
              println(s"Error evaluating expression '$expr' and '$replacedExpr': 异常点：${ex.getMessage}")
              (expr, replacedExpr, None, timestamp) // 捕获异常时返回 None
          }
        }
      }
    }.toSeq
    groupdata
  }


  // 辅助函数：严格单调递增
  def isStrictlyIncreasing(arr: Array[Double]): Boolean = {
    // 边界检查：空数组或单元素数组
    if (arr.isEmpty) {
      // 空数组无法判断是否严格递减，返回 false 或抛出异常，视需求而定
      return false
    }
    if (arr.length == 1) {
      // 单元素数组认为是严格递减的
      return true
    }
    arr.zip(arr.tail).forall { case (a, b) => a < b }
  }

  // 辅助函数：严格单调递减
  def isStrictlyDecreasing(arr: Array[Double]): Boolean = {
    // 边界检查：空数组或单元素数组
    if (arr.isEmpty) {
      // 空数组无法判断是否严格递减，返回 false 或抛出异常，视需求而定
      return false
    }
    if (arr.length == 1) {
      // 单元素数组认为是严格递减的
      return true
    }
    arr.zip(arr.tail).forall { case (a, b) => a > b }
  }

  // 辅助函数：严格保持不变（所有元素相同）
  def isStrictlyConstant(arr: Array[Double]): Boolean = {
    // 边界检查：空数组或单元素数组
    if (arr.isEmpty) {
      // 空数组无法判断是否严格递减，返回 false 或抛出异常，视需求而定
      return false
    }
    if (arr.length == 1) {
      // 单元素数组认为是严格递减的
      return true
    }
    arr.zip(arr.tail).forall { case (a, b) => a == b }
  }


  private def updateResponse(response: IoTDBReading, rulesForDevice: ListBuffer[DiagnosisRule], context: CoProcessFunction[String, (String, InputData), OutResult]#Context): IoTDBReading = {
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
          context.output(outputTag, diagnosisResult)
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
  //精度转换要求

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
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(120).toEpochMilli
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case (Some(lastTs), Some(lastTs2)) if lastTs < lastTs2 =>
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${lastTs} AND time < ${lastTs2}"
      case (Some(lastTs), _) =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(0).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(120).toEpochMilli
        s"SELECT ${iotFlds} FROM ${tableKey} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case _ =>
        s"SELECT ${iotFlds} FROM ${tableKey}  where time>=${timestamp} ORDER BY time asc LIMIT 4 "
    }
  }


  private def get_data_incre(data:String,iot_tbl:String):  ListBuffer[DiagnosisRule] = {
    // 指定驱动
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"
    Class.forName(driver)
    // 创建数据库连接
    connection = DriverManager.getConnection(url, user, password)

    connection.setAutoCommit(false) // 并不是所有数据库都适用，比如hive就不支持，orcle不需要
    //获取首次最大时间
    // 准备增量查询语句
    val incrementalQuery =
      s"""
         |SELECT *,CASE
         |        WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2 THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
         |        ELSE split_part(iot_fld, '_', 1)
         |    END AS iot_dev
         |FROM public.kr_diagnosis_rules
         |WHERE iot_fld in('${data.replace(",","','")}')
         |and iot_tbl in('$iot_tbl')
         |order by site_id
         |""".stripMargin
    val psIncremental = connection.prepareStatement(incrementalQuery)
    val incrementalResultSet = psIncremental.executeQuery()
    // 将 ResultSet 中的数据存储到列表中
    val rules = scala.collection.mutable.ListBuffer[DiagnosisRule]()
    while (incrementalResultSet.next()) {
      val rule = DiagnosisRule(
        id = incrementalResultSet.getLong("id"),
        site_id = incrementalResultSet.getString("site_id"),
        iot_tbl = incrementalResultSet.getString("iot_tbl"),
        iot_fld = incrementalResultSet.getString("iot_fld"),
        src_disconn = incrementalResultSet.getBoolean("src_disconn"),
        samp_freq_mismatch = incrementalResultSet.getBoolean("samp_freq_mismatch"),
        samp_freq_diag_time = incrementalResultSet.getLong("samp_freq_diag_time"),
        samp_freq_clr_time = incrementalResultSet.getLong("samp_freq_clr_time"),
        conv_amp_factor_sign = incrementalResultSet.getBoolean("conv_amp_factor_sign"),
        conv_amp_factor = incrementalResultSet.getDouble("conv_amp_factor"),
        norm_val_sign = incrementalResultSet.getBoolean("norm_val_sign"),
        norm_val = incrementalResultSet.getDouble("norm_val"),
        auto_mon_max_val = incrementalResultSet.getBoolean("auto_mon_max_val"),
        auto_clr_max_val = incrementalResultSet.getBoolean("auto_clr_max_val"),
        max_val_thres = incrementalResultSet.getDouble("max_val_thres"),
        max_val_estab_time = incrementalResultSet.getLong("max_val_estab_time"),
        max_val_clr_time = incrementalResultSet.getLong("max_val_clr_time"),
        auto_mon_min_val = incrementalResultSet.getBoolean("auto_mon_min_val"),
        auto_clr_min_val = incrementalResultSet.getBoolean("auto_clr_min_val"),
        min_val_thres = incrementalResultSet.getDouble("min_val_thres"),
        min_val_estab_time = incrementalResultSet.getLong("min_val_estab_time"),
        min_val_clr_time = incrementalResultSet.getLong("min_val_clr_time"),
        auto_mon_rate_chg = incrementalResultSet.getBoolean("auto_mon_rate_chg"),
        auto_clr_rate_chg = incrementalResultSet.getBoolean("auto_clr_rate_chg"),
        rate_chg_thres = incrementalResultSet.getDouble("rate_chg_thres"),
        rate_chg_estab_time = incrementalResultSet.getLong("rate_chg_estab_time"),
        rate_chg_clr_time = incrementalResultSet.getLong("rate_chg_clr_time"),
        auto_mon_dead_zone = incrementalResultSet.getBoolean("auto_mon_dead_zone"),
        auto_clr_dead_zone = incrementalResultSet.getBoolean("auto_clr_dead_zone"),
        dead_zone_thres_z1 = incrementalResultSet.getDouble("dead_zone_thres_z1"),
        dead_zone_thres_v1 = incrementalResultSet.getDouble("dead_zone_thres_v1"),
        dead_zone_thres_v2 = incrementalResultSet.getDouble("dead_zone_thres_v2"),
        dead_zone_thres_v3 = incrementalResultSet.getDouble("dead_zone_thres_v3"),
        created_at = incrementalResultSet.getTimestamp("created_at"),
        updated_at = incrementalResultSet.getTimestamp("updated_at"),
        storage_type = incrementalResultSet.getString("storage_type"),
        iot_dev = incrementalResultSet.getString("iot_dev")
      )
      rules += rule
    }
    incrementalResultSet.close()
    psIncremental.close()
    connection.close()
    rules
  }

  import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
  import scala.collection.mutable.ListBuffer
  import org.postgresql.util.PSQLException

  private def get_data(iot_tbl: String): ListBuffer[DiagnosisRule] = {
    // 指定驱动
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"
    var connection: Connection = null
    var psIncremental: PreparedStatement = null
    var incrementalResultSet: ResultSet = null

    try {
      Class.forName(driver)
      // 创建数据库连接
      connection = DriverManager.getConnection(url, user, password)
      connection.setAutoCommit(false)

      // 准备增量查询语句
      val incrementalQuery =
        s"""
           |SELECT *,CASE
           |        WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2 THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
           |        ELSE split_part(iot_fld, '_', 1)
           |    END AS iot_dev
           |FROM public.kr_diagnosis_rules
           |order by site_id
           |""".stripMargin

//      println(s"打印sql:${incrementalQuery}")
      val maxRetries = 3 // 最大重试次数
      var retries = 0
      var success = false
      var rules = scala.collection.mutable.ListBuffer[DiagnosisRule]()

      while (!success && retries < maxRetries) {
        try {
          psIncremental = connection.prepareStatement(incrementalQuery)
          incrementalResultSet = psIncremental.executeQuery()

          while (incrementalResultSet.next()) {
            val rule = DiagnosisRule(
              id = incrementalResultSet.getLong("id"),
              site_id = incrementalResultSet.getString("site_id"),
              iot_tbl = incrementalResultSet.getString("iot_tbl"),
              iot_fld = incrementalResultSet.getString("iot_fld"),
              src_disconn = incrementalResultSet.getBoolean("src_disconn"),
              samp_freq_mismatch = incrementalResultSet.getBoolean("samp_freq_mismatch"),
              samp_freq_diag_time = incrementalResultSet.getLong("samp_freq_diag_time"),
              samp_freq_clr_time = incrementalResultSet.getLong("samp_freq_clr_time"),
              conv_amp_factor_sign = incrementalResultSet.getBoolean("conv_amp_factor_sign"),
              conv_amp_factor = incrementalResultSet.getDouble("conv_amp_factor"),
              norm_val_sign = incrementalResultSet.getBoolean("norm_val_sign"),
              norm_val = incrementalResultSet.getDouble("norm_val"),
              auto_mon_max_val = incrementalResultSet.getBoolean("auto_mon_max_val"),
              auto_clr_max_val = incrementalResultSet.getBoolean("auto_clr_max_val"),
              max_val_thres = incrementalResultSet.getDouble("max_val_thres"),
              max_val_estab_time = incrementalResultSet.getLong("max_val_estab_time"),
              max_val_clr_time = incrementalResultSet.getLong("max_val_clr_time"),
              auto_mon_min_val = incrementalResultSet.getBoolean("auto_mon_min_val"),
              auto_clr_min_val = incrementalResultSet.getBoolean("auto_clr_min_val"),
              min_val_thres = incrementalResultSet.getDouble("min_val_thres"),
              min_val_estab_time = incrementalResultSet.getLong("min_val_estab_time"),
              min_val_clr_time = incrementalResultSet.getLong("min_val_clr_time"),
              auto_mon_rate_chg = incrementalResultSet.getBoolean("auto_mon_rate_chg"),
              auto_clr_rate_chg = incrementalResultSet.getBoolean("auto_clr_rate_chg"),
              rate_chg_thres = incrementalResultSet.getDouble("rate_chg_thres"),
              rate_chg_estab_time = incrementalResultSet.getLong("rate_chg_estab_time"),
              rate_chg_clr_time = incrementalResultSet.getLong("rate_chg_clr_time"),
              auto_mon_dead_zone = incrementalResultSet.getBoolean("auto_mon_dead_zone"),
              auto_clr_dead_zone = incrementalResultSet.getBoolean("auto_clr_dead_zone"),
              dead_zone_thres_z1 = incrementalResultSet.getDouble("dead_zone_thres_z1"),
              dead_zone_thres_v1 = incrementalResultSet.getDouble("dead_zone_thres_v1"),
              dead_zone_thres_v2 = incrementalResultSet.getDouble("dead_zone_thres_v2"),
              dead_zone_thres_v3 = incrementalResultSet.getDouble("dead_zone_thres_v3"),
              created_at = incrementalResultSet.getTimestamp("created_at"),
              updated_at = incrementalResultSet.getTimestamp("updated_at"),
              storage_type = incrementalResultSet.getString("storage_type"),
              iot_dev = incrementalResultSet.getString("iot_dev")
            )
            rules += rule
          }
          success = true
        } catch {
          case e: PSQLException if e.getMessage.contains("An I/O error occurred while sending to the backend.") =>
            retries += 1
            println(s"捕获到 I/O 错误，第 $retries 次重试，等待 1 秒后重新执行查询...")
            Thread.sleep(1000)
          case e: Exception =>
            throw e
        } finally {
          if (incrementalResultSet != null) incrementalResultSet.close()
          if (psIncremental != null) psIncremental.close()
        }
      }
      if (!success) {
        throw new RuntimeException("经过多次重试后，仍然无法成功执行查询。")
      }

      connection.close()
      rules
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def filterRules(rules: ListBuffer[DiagnosisRule], data: String, iot_tbl: String): ListBuffer[DiagnosisRule] = {
    val parts = iot_tbl.split("_")
    val tableKey = if (parts.length > 1) {
      parts.init.mkString("_") // 所有除最后一个部分外拼接回来
    } else {
      iot_tbl
    }
    val dataFields = data.split(",").map(_.trim).toSet
    rules.filter(rule => dataFields.contains(rule.iot_fld) && rule.iot_tbl == tableKey)
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

  override def processElement1(input1: String, context: CoProcessFunction[String, (String, InputData), OutResult]#Context, out: Collector[OutResult]): Unit = {
    //直接从状态管理器利里边拿到数值进行校验
    var input_size=0
    if (rulesState.contains(input1)) {
      input_size = rulesState.get(input1).size
    }
    // 安全访问 Map
    val site_length: Int = {
      if (inputLengthState != null && input1 != null&&inputLengthState.contains(input1))
        inputLengthState.get(input1)
      else 0
    }
//    logger.info(s"打印长度：${site_length},input长度：${input_size} 站點數據：${input1}")

    if(site_length==input_size&&site_length!=0){
      val start_time =System.currentTimeMillis()
      // 获取 position 组成的列表
//      val position: List[String] = rulesState.get(input1).map(_.position).toList
      //从里边筛选出要校验的值名称是否含有物理值字段的属性
//      println(s"打印input的数值：$input1")
      val filter_data =rulesState.get(input1)
      //设备时间
      val faStartime = Option(facilityStartTime.value()).getOrElse(Map.empty[String, (Some[Long],Some[Long])])

      // 判断是否为首次执行
      val isFirstExecution = faStartime.isEmpty || !faStartime.contains(input1)
      logger.info(s"打印是否是首行：${input1}:${isFirstExecution}:${faStartime} 线程名称：${Thread.currentThread().getName}")

      val regex = """`(\d+)`""".r
      val result = regex.findFirstMatchIn(input1).map(_.group(1)).getOrElse("")
//      val matchingExpressions = expressionsState.get().filter(data => data.contains(result))


      val scripts=scriptsState.get().filter(data=>data.contains(result)).toSeq
      /**
       * 重新分区的写法 要是存在的话就直接将那个进行替代即可
       */
//      if(input1=="root.ln.`1523723612516384521`_2")  {
//        println(input1)
//      }

      val matchingExpressions = expressionsState.get(input1).toSeq
//      println(s"打印这个数值：${matchingExpressions.size}")
      val sqlQuery: Seq[Seq[(String, String, Any, Long)]] =if(input1.contains("1821385107949223936")||input1.contains("1269014857442188595")||input1.contains("1816341324371066880")||input1.contains()){
        //要是重分区的话这个是要调整的
        val matchExpression = matchingExpressions.filter(data=> data.contains(filter_data.map(_.position)))
        buildSqlQuery(filter_data,matchExpression,scripts, faStartime,isFirstExecution,context,input1)
      }else{
        buildSqlQuery(filter_data,matchingExpressions,scripts, faStartime,isFirstExecution,context,input1)
      }

      // 拼接SQL读取业务数据（假设这是你需要的部分）
//      val sqlQuery02 = buildSqlQuery(filter_data,matchingExpressions,scripts, faStartime,isFirstExecution,input1)

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
//                else {
//                  // 时间跨度不足，继续收集数据
//
//                  continue_data.put(exp_key,updatedState)
//                  out.collect(OutResult(
//                    timestamp = timestamp,
//                    expressions = expression,
//                    collectedValue = Some(false),
//                    alarm = alarm,
//                    level=level,
//                    station = station,
//                    properties=properties,
//                    alg_desc=alg_desc,
//                    alg_label_EN=alg_label_EN,
//                    alg_brief_CH=alg_brief_CH,
//                    alg_param=alg_param,
//                    alg_show_group=alg_show_group,
//                    alg_show_levelcursor=alg_show_levelcursor
//                  ))
//                }
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

      logger.info(s"Processed data:${System.currentTimeMillis()-start_time}  key:${input1} 数据结束时间")
    }

  }

  override def processElement2(input2: (String, InputData), context: CoProcessFunction[String, (String, InputData), OutResult]#Context, collector: Collector[OutResult]): Unit = {
    //接收文件读到的数据
    val key = input2._1
    val newInputDataSet = Set(input2._2)

    if (rulesState.contains(key)) {
      val existingInputDataSet = rulesState.get(key)
      val updatedInputDataSet = existingInputDataSet ++ newInputDataSet
      // 将文件存储至状态管理器里边
      rulesState.put(key, updatedInputDataSet)
    } else {
      rulesState.put(key, newInputDataSet)
    }

    val isRulesLoaded = isRulesLoadedState.value()
    if (!isRulesLoaded) {
      loadRules(key)
      val parts = key.split("_")
      val tableKey = if (parts.length > 1) {
        parts.init.mkString("_") // 所有除最后一个部分外拼接回来
      } else {
        key
      }
      rule_data=get_data(tableKey)
      isRulesLoadedState.update(true)
    }
  }
}