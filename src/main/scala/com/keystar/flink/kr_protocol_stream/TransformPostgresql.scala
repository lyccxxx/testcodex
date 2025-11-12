package com.keystar.flink.kr_protocol_stream

import com.keystar.flink.iotdbfunction.IotdbFunction.isFalseOrTrue
import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import com.keystar.flink.kr_json.OutResult
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.slf4j.LoggerFactory

import java.sql.DriverManager
import java.time.Instant
import java.util.Date
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class TransformPostgresql(timestamp: Long,sitepartion:Map[String,Int])  extends KeyedProcessFunction[String, (String, KrProtocolData), OutputMq] {
  // 用于存储传入的业务数据
  private var incomingData: ListState[KrProtocolData] = _  // 新流入的数据
  // 用于存储定时器的时间戳
  private var timerState: ValueState[Long] = _
  // 存储每个键的数据量

  private val sitePartitionMap=sitepartion
  // 存储每个站点的总数据量
  private val siteTotalCounts = Map(
    "root.ln.`1521412352616268498`" -> 192049L,
    "root.ln.`1521714652626283467`" -> 28147L,
    "root.ln.`1522125219418180352`" -> 13557L,
    "root.ln.`1523723612516384521`" -> 38631L
  )
  // 定时器间隔（建议5分钟，可配置）
  private val REFRESH_INTERVAL = 12* 60 * 60 * 1000L

  // 使用 MapState 存储站点总数据量（支持动态更新）
  private var siteTotalState: MapState[String, Long] = _

  // 记录定时器是否已注册
  private var timerRegistered: ValueState[Boolean] = _
  // 计数器
  private var counterState: ValueState[Long] = _
  // 故障状态管理器
  private lazy val faultStates: ValueState[Map[String, (Option[Long], Option[Long], String)]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long], String)]]("faultStates", classOf[Map[String, (Option[Long], Option[Long], String)]])
  )
  // 设备启动时间管理器
  private lazy val facilityStartTime: ValueState[Map[String, (Option[Long], Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long])]]("facilityStartTime", classOf[Map[String, (Option[Long], Option[Long])]])
  )

  private val logger = LoggerFactory.getLogger(this.getClass)



  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 初始化状态
    incomingData = getRuntimeContext.getListState(
      new ListStateDescriptor[KrProtocolData]("incomingData", classOf[KrProtocolData]))

    // 初始化状态描述符
    val descriptor = new MapStateDescriptor[String, Long](
      "siteTotalState",
      TypeInformation.of(classOf[String]),
      TypeInformation.of(classOf[Long])
    )
    siteTotalState = getRuntimeContext.getMapState(descriptor)

    timerState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timerState", classOf[Long]))

    counterState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("counter", classOf[Long], 0L))



    timerRegistered = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("timerRegistered", classOf[Boolean], false))

  }

  override def processElement(
                               input: (String, KrProtocolData),
                               ctx: KeyedProcessFunction[String, (String, KrProtocolData), OutputMq]#Context,
                               out: Collector[OutputMq]
                             ): Unit = {
    val currentKey = ctx.getCurrentKey()


    // 增加计数器
    val count = counterState.value() + 1
    counterState.update(count)


    // 从键中提取站点ID和分区号
    val Array(siteId, partitionIdStr) = input._1.split("_", 2)
    val partitionId = partitionIdStr.toInt

    // 首次处理该键时加载数据库数据（避免重复加载）
    if (siteTotalState.get(siteId) == null) {  // 基于当前键检查状态是否存在
      loadSiteTotalCountsFromDB()
    }

    // 获取该站点的分区数和总数据量
    val numPartitions = sitePartitionMap.getOrElse(siteId, 1)
    val totalCount =siteTotalState.get(siteId)
    // 计算每个分区的确切预期数据量
    val basePerPartition = totalCount / numPartitions
    val remainder = totalCount % numPartitions

    // 确定当前分区的预期数据量
    // 分区ID从0开始，因此前remainder个分区（0到remainder-1）会多一条数据
    val expectedCount = if (partitionId < remainder) {
      basePerPartition + 1
    } else {
      basePerPartition
    }


    // 将数据添加到新流入数据
    incomingData.add(input._2)

    // 获取当前批次和新流入数据的总大小
    val currentSize = incomingData.get().asScala.size

    if (timerRegistered.value() == null || !timerRegistered.value()) {
      // 注册定时器，确保即使数据持续流入也会定期触发
      registerTimer(ctx) // 12小时间隔
      timerRegistered.update(true)
      // 首次处理该键时加载数据库数据（避免重复加载）
    }

    // 当incomingData达到批次大小时，处理并清空
    if (count == expectedCount) {

      // 获取当前所有数据
      val allData = incomingData.get().asScala.toList

      // 计算需要处理的批次数量
      val batchCount = math.ceil(currentSize.toDouble / 1008).toInt
      // 分批处理数据
      for (i <- 0 until batchCount) {
        val startIdx = i * 1008
        val endIdx = math.min((i + 1) * 1008, currentSize)
        val batch = allData.slice(startIdx, endIdx)

        // 判断是否为最后一个批次
        val isLastBatch = (i == batchCount - 1)
        processBatch(out, isLastBatch, currentKey, batch)
//        logger.info(s"Processing batch ${i+1}/${batchCount} for key $currentKey, size: ${batch.size}")
      }
      MemoryMonitor.logMemoryUsage(s"Processing key: ${ctx.getCurrentKey} 已完成处理所有预期数据 (${expectedCount}/${totalCount}) 当前并行情况：${Thread.currentThread().getName}")


      // 清空状态
      incomingData.clear()
      counterState.clear()
//      logger.info(s"信息: 分区 ${input._1} 已完成处理所有预期数据 (${expectedCount}/${totalCount})")
      // 记录批次处理完成
      clearAllStates(ctx)
    }
  }



  private def registerTimer(ctx: KeyedProcessFunction[String, (String, KrProtocolData), OutputMq]#Context): Unit = {
    try {
      val currentTimer = timerState.value()
      if (currentTimer > 0) {
        ctx.timerService().deleteProcessingTimeTimer(currentTimer)
      }

      // 根据数据活跃度动态调整定时器时间

      val timerTimestamp = ctx.timerService().currentProcessingTime() + REFRESH_INTERVAL
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      timerState.update(timerTimestamp)
      timerRegistered.update(true)

//      logger.info(s"Timer registered for key: ${ctx.getCurrentKey()}, delay=${timerDelay/1000}s, will trigger at ${new Date(timerTimestamp)}")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to register timer for key: ${ctx.getCurrentKey()}", e)
        timerState.clear()
        timerRegistered.clear()
    }
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, (String, KrProtocolData), OutputMq]#OnTimerContext,
                        out: Collector[OutputMq]
                      ): Unit = {
    val deviceId = ctx.getCurrentKey()
    try {
        loadSiteTotalCountsFromDB()
    } catch {
      case e: Exception =>
        logger.error(s"Error processing timer for device: $deviceId", e)
    } finally {
      // 无论处理成功与否，都重新注册定时器
      registerTimer(ctx)
    }
  }

  private def loadSiteTotalCountsFromDB(): Unit = {
    // 从PG库查询最新数据（使用JDBC连接）
    val url02 = "jdbc:postgresql://172.16.1.34:5432/data"
    val url = "jdbc:postgresql://10.190.6.97:5432/data"  //高井
    val user = "postgres"
    val password = "K0yS@2024"
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    val iot_table = sitepartion.keySet.toList
    val connection = DriverManager.getConnection(url, user, password)
    val query =
      s"""select iot_table ,count(1) as total_count
        |from public.kr_protocol_data
        |where  site_id!='-1' and validation ='Y'
        |and iot_table in ('${iot_table.map(_.replace("'", "''")).mkString("', '")}')
        |-- and iot_field in('I006_16_INVERTER_Line16_I','I006_14_INVERTER_Room_Tmp')
        |group by iot_table""".stripMargin
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)

    val newTotalCounts = mutable.Map[String, Long]()
    while (resultSet.next()) {
      newTotalCounts(resultSet.getString("iot_table")) = resultSet.getLong("total_count")
    }

    // 更新状态
    if (!siteTotalState.isEmpty) {
      siteTotalState.clear()
    }

    newTotalCounts.foreach { case (k, v) => siteTotalState.put(k, v) }

    connection.close()
    statement.close()
    resultSet.close()
  }

  private def clearAllStates(ctx: KeyedProcessFunction[String, (String, KrProtocolData), OutputMq]#Context): Unit = {
    // 取消定时器
    val currentTimer = timerState.value()
    if (currentTimer > 0) {
      ctx.timerService().deleteProcessingTimeTimer(currentTimer)
    }

    // 清空状态
    incomingData.clear()
    timerState.clear()
    timerRegistered.clear()
  }

  private def processBatch(out: Collector[OutputMq], isTimerTrigger: Boolean, keynum: String, batchData: List[KrProtocolData]): Unit = {
    if (batchData.nonEmpty) {
      // 获取设备启动时间
//      logger.info(s"打印数值情况：${keynum} 长度：${batchData.size} 数值情况：${isTimerTrigger} 当前并行情况：${Thread.currentThread().getName} ")
      val faStartime = Option(facilityStartTime.value()).getOrElse(Map.empty[String, (Some[Long],Some[Long])])
      val key_String=s"root.ln.`${keynum.toString}`"
      val isFirstExecution = faStartime.isEmpty || !faStartime.contains(key_String)
      val (lastTimestampOpt, lastTimestampOpt2) = if (isFirstExecution) {
        (None, None)
      } else {
        faStartime.get(key_String) match {
          case Some((opt1, opt2)) => (opt1, opt2)
          case None => (None, None)
        }
      }

      // 提取 IoT 字段和设备名称
      val iotFlds = extractIotFields(batchData)
      val device = extractDeviceNames(batchData)

      // 构造 SQL 查询
//      logger.info(s"时间戳的数值情况：${lastTimestampOpt} ${lastTimestampOpt2}")
      val batchsql = getSqlQuery(lastTimestampOpt, iotFlds, device,lastTimestampOpt2)
//      println(s"打印sql:$batchsql")
      val result = sendRequest(batchsql)
      // 处理响应
      val responses: List[IoTDBReading] = handleResponse(result)

      if (responses.isEmpty) {
        // 如果结果为空，强制更新时间戳
        val lastTimestampOpts = lastTimestampOpt.map(_ + 300000) // 增加 300 秒（5 分钟）
        facilityStartTime.update(Map(key_String -> (lastTimestampOpt,lastTimestampOpts)))
      }

      if (responses.nonEmpty && isTimerTrigger) {
        // 更新设备启动时间
        val timestamp = responses.map(_.timestamp).max
        updateFacilityStartTime(faStartime, keynum.toString, timestamp)
        facilityStartTime.update(faStartime + (key_String -> (Some(timestamp),Some(timestamp))))
      }


      // 处理每条响应
      responses.sortBy(_.timestamp).foreach { response =>
        processResponse(out, response, batchData)
      }
    }
  }

  private def getFacilityStartTime(): Map[String, (Option[Long],Option[Long])] = {
    Option(facilityStartTime.value()).getOrElse(Map.empty[String, (Option[Long],Option[Long])])
  }

  private def extractIotFields(batchData: List[KrProtocolData]): String = {
    batchData.flatMap(data => data.iot_field).mkString(",")
  }

  private def extractDeviceNames(batchData: List[KrProtocolData]): String = {
    batchData.flatMap(data => data.iot_table).distinct.mkString(",")
  }

  private def updateFacilityStartTime(faStartime: Map[String, (Option[Long],Option[Long])], device: String, timestamp: Long): Unit = {
    val key_String=s"root.ln.`${device.toString}`"
    facilityStartTime.update(faStartime + (key_String -> (Some(timestamp),Some(timestamp))))
  }

  //记录状态
  private def getFaultStates(): Map[String, (Option[Long], Option[Long], String)] = {
    Option(faultStates.value()).getOrElse(Map.empty[String, (Option[Long], Option[Long], String)])
  }

  private def processResponse(out: Collector[OutputMq], response: IoTDBReading, batchData: List[KrProtocolData]): Unit = {
    for ((key, value) <- response.values) {
      // 提取字段名称
      val fieldName = key.split("\\.").lastOption.getOrElse("")
      val iot_table = key.split("\\.").dropRight(1).mkString(".")

      // 过滤出与当前字段相关的规则数据
      val ruleDf = batchData.filter(data => data.iot_field.exists(_.endsWith(fieldName)))

      ruleDf.foreach { rudata =>
        // 判断数据是否超出范围
        val boolValue = isFalseOrTrue(value)
        if (!boolValue && rudata.alarm.contains("Y")) {
          toDouble(value).foreach { num =>
            val tranvalue02 = convertValue(num.toString, rudata.storage_type.getOrElse(""), rudata.precision.map(_.toString))
            toDouble(tranvalue02).foreach { tranvalue =>
              val alertLevel = getAlertLevel(tranvalue, rudata)
              val fault = getFaultStates()
              fault.get(key) match {
                case Some((start_time, end_time, result)) =>
                  if (alertLevel == "WARNING" || alertLevel == "SERIOUS") {
                    // 非首次且为WARNING或SERIOUS时
                    if (start_time.isDefined) {
                      // 更新时间
                      val updatedFaultState = fault + (key -> (start_time, Some(response.timestamp), alertLevel))
                      faultStates.update(updatedFaultState)
                    } else {
                      // 首次出现WARNING或SERIOUS时记录时间戳并发送数据
                      val updatedFaultState = fault + (key -> (Some(response.timestamp), None, alertLevel))
                      faultStates.update(updatedFaultState)
                      out.collect(OutputMq(timestamp = response.timestamp,startTime =response.timestamp,currentType = 0,dataType =rudata.data_type.getOrElse("")
                        ,deviceId =rudata.device_id.getOrElse(0L) , iot_table = iot_table,
                        iot_field = fieldName, alarm_level = alertLevel,endTime=0L))
                    }
                  } else {
                    // 当alertLevel不为WARNING或SERIOUS时，如果key存在则移除并发送数据
                    if(fault.contains(key)){
                      if(end_time.nonEmpty){
                        out.collect(OutputMq(timestamp = response.timestamp,startTime =start_time.getOrElse(0L),endTime=end_time.getOrElse(0L)
                          ,currentType = 1,dataType =rudata.data_type.getOrElse("")
                          ,deviceId =rudata.device_id.getOrElse(0L) , iot_table = iot_table,
                          iot_field = fieldName, alarm_level = "ALARM"))
                      }
                      faultStates.update(fault - key)
                    }
                  }
                case None =>
                  // 如果是首次遇到WARNING或SERIOUS，则记录时间戳并发送数据
                  if (alertLevel == "WARNING" || alertLevel == "SERIOUS") {
                    out.collect(OutputMq(timestamp = response.timestamp,startTime =response.timestamp,endTime=0L
                      ,currentType = 0,dataType =rudata.data_type.getOrElse("")
                      ,deviceId =rudata.device_id.getOrElse(0L) , iot_table = iot_table,
                      iot_field = fieldName, alarm_level = alertLevel))
                    val updatedFaultState = fault + (key -> (Some(response.timestamp), None, alertLevel))
                    faultStates.update(updatedFaultState)
                  }
                // 其他情况不处理
              }
            }
          }
        }
      }
    }
  }

  private def getAlertLevel(tranvalue: Double, rudata: KrProtocolData): String = {
    // 先检查严重上下限
    val criticalLower = rudata.critical_lower_limit
    val criticalUpper = rudata.critical_upper_limit

    val isCritical = (criticalLower, criticalUpper) match {
      case (Some(lower), Some(upper)) => tranvalue < lower || tranvalue > upper
      case (Some(lower), None)        => tranvalue < lower
      case (None, Some(upper))        => tranvalue > upper
      case _                          => false
    }

    if (isCritical) return "SERIOUS"

    // 再检查警告上下限
    val warningLower = rudata.warning_lower_limit
    val warningUpper = rudata.warning_upper_limit

    val isWarning = (warningLower, warningUpper) match {
      case (Some(lower), Some(upper)) => tranvalue < lower || tranvalue > upper
      case (Some(lower), None)        => tranvalue < lower
      case (None, Some(upper))        => tranvalue > upper
      case _                          => false
    }

    if (isWarning) return "WARNING"

    // 都未触发则返回正常
    "NORMAL"
  }

  // 将任意类型的值转换为 Option[Double]
  private def toDouble(value: Any): Option[Double] = value match {
    case Some(v) => toDouble(v) // 递归调用 toDouble 处理 Option 内部的值
    case n: Number => Some(n.doubleValue())
    case s: String => Try(s.toDouble).toOption
    case _ =>
      None
  }

  // 精度转换要求
  def convertValue(valueStr: String, dataType: String, accuracy: Option[String]): Any = {
    val parsedAccuracy = accuracy.flatMap(acc => Try(acc.toDouble).toOption) // 尝试将精度字符串转换为 Double
    dataType match {
      case "float" | "double" =>
        if (valueStr == null || valueStr.isEmpty) {
          None
        } else {
          val value = valueStr.toDouble
          parsedAccuracy match {
            case Some(acc) => Some((math rint (value / acc)) * acc) // 根据精度四舍五入
            case None => Some(value)
          }
        }
      case "int" | "integer" =>
        if (valueStr == null || valueStr.isEmpty) {
          None
        } else {
          Try(valueStr.toInt).toOption
        }
      case "boolean" =>
        if (valueStr == null || valueStr.isEmpty) {
          None
        } else {
          Try(valueStr.toBoolean).toOption
        }
      case _ =>
        if (valueStr == null || valueStr.isEmpty) {
          None
        } else {
          Some(valueStr)
        }
    }
  }

  private def getSqlQuery(lastTsOpt: Option[Long], iotFlds: String, device: String, lastTsOpt02: Option[Long]): String = {
    (lastTsOpt, lastTsOpt02) match {
      case (Some(lastTs), Some(lastTs2)) if lastTs == lastTs2 =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(0).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(210).toEpochMilli
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case (Some(lastTs), Some(lastTs2)) if lastTs < lastTs2 =>
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${lastTs} AND time < ${lastTs2}"
      case (Some(lastTs), _) =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(0).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(210).toEpochMilli
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case _ =>
        s"SELECT ${iotFlds} FROM ${device} where time  >=${timestamp} order by time asc limit 10"
    }
  }
}