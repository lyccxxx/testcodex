package com.keystar.flink.kr_protocol_stream

import com.keystar.flink.iotdbfunction.IotdbFunction.isFalseOrTrue
import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import com.keystar.flink.kr_json.OutResult
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.Try

// 在 KrProtocolData 类中增加时间戳字段
class transform_postgresql_optimized extends KeyedProcessFunction[Long, (Long, KrProtocolData), OutputMq] {
  // 用于存储传入的业务数据
  private var StateInputData: ListState[KrProtocolData] = _
  // 用于存储定时器的时间戳
  private var timerState: ValueState[Long] = _

  private var errorState: ListState[OutputMq] = _

  private val logger = LoggerFactory.getLogger(this.getClass)
  //计数器
  private var counterState: ValueState[Long] = _

  private lazy val faultStates: ValueState[Map[String, (Option[Long], Option[Long], String)]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long], String)]]("faultStates", classOf[Map[String, (Option[Long], Option[Long], String)]])
  )
  // 状态管理器，用于记录站点、设备，上次跑的时间
  private lazy val facilityStartTime: ValueState[Map[String, (Option[Long], Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long])]]("facilityStartTime", classOf[Map[String, (Option[Long], Option[Long])]])
  )

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 初始化状态
    StateInputData = getRuntimeContext.getListState(
      new ListStateDescriptor[KrProtocolData]("resultState", classOf[KrProtocolData]))
    timerState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timerState", classOf[Long]))
    val descriptor = new ValueStateDescriptor[Long]("counter", classOf[Long], 0L)
    counterState = getRuntimeContext.getState(descriptor)
  }

  override def processElement(
                               input: (Long, KrProtocolData),
                               ctx: KeyedProcessFunction[Long, (Long, KrProtocolData), OutputMq]#Context,
                               out: Collector[OutputMq]
                             ): Unit = {
    val startTime = System.currentTimeMillis()
    // 将数据添加到状态中
    StateInputData.add(input._2)
    var count = counterState.value()
    count += 1
    counterState.update(count)
    if (count == 1) {
//      logger.info(s"Processed data: 数据开始时间范围：数据结束时间 ${Thread.currentThread().getName}")
    }

    // 取消之前的定时器
    val currentTimer = timerState.value()
    if (currentTimer > 0) {
      ctx.timerService().deleteProcessingTimeTimer(currentTimer)
    }

    // 注册一个新的定时器
    val timerTimestamp = ctx.timerService().currentProcessingTime() + 10000 // 20秒后触发
    ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
    timerState.update(timerTimestamp)

    // 如果状态中的数据达到批次大小（例如2000条），则立即触发处理
    if (StateInputData.get().asScala.size >= 2000) {
      processBatch(out, false, input._1)
      // 清空状态并取消定时器
      StateInputData.clear()
      ctx.timerService().deleteProcessingTimeTimer(timerState.value())
      timerState.clear()
      val endTime = System.currentTimeMillis()
      var count2 = counterState.value()
//      logger.info(s"Processed data:$count2   key:${ctx.getCurrentKey() } , Time taken: ${endTime - startTime} ms 开始时间：${Thread.currentThread().getName}")
    }
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[Long, (Long, KrProtocolData), OutputMq]#OnTimerContext,
                        out: Collector[OutputMq]
                      ): Unit = {
    if (StateInputData.get().asScala.nonEmpty) {
      processBatch(out, true, ctx.getCurrentKey)
      // 清空状态
      //在这里边的时候是不是可以考虑一下更新时间戳
      StateInputData.clear()
      timerState.clear()
      val endTime = System.currentTimeMillis()
      var count2 = counterState.value()
      logger.info(s"Processed data:$count2   key:${ctx.getCurrentKey() } , Time taken: ${endTime - timestamp} ms 定时器触发时间：$timestamp  ${Thread.currentThread().getName}")
    }
  }

  private def processBatch(out: Collector[OutputMq], num: Boolean, keynum: Long): Unit = {
    // 获取状态中的所有数据，并按时间戳排序
    val batchData = StateInputData.get().iterator().asScala.toList
    if (batchData.nonEmpty) {
      // 获取设备启动时间
      val faStartime = Option(facilityStartTime.value()).getOrElse(Map.empty[String, (Some[Long], Some[Long])])
      val key_String = s"root.ln.`${keynum.toString}`"
      val isFirstExecution = faStartime.isEmpty || !faStartime.contains(key_String)
      val (num01, num02) = faStartime.get(key_String) match {
        case Some((opt1, opt2)) => (opt1, opt2)
        case None => (None, None)
      }
      println(s"打印数值情况：${num01} ${num02},主键key：${keynum}  ${isFirstExecution} ${facilityStartTime.value()} ${Thread.currentThread().getName}")
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
      val batchsql = getSqlQuery(lastTimestampOpt, iotFlds, device, lastTimestampOpt2)
      val result = sendRequest(batchsql)
      // 处理响应
      val responses: List[IoTDBReading] = handleResponse(result)
      if (responses.isEmpty) {
        // 如果结果为空，强制更新时间戳
        val lastTimestampOpts = lastTimestampOpt.map(_ + 300000) // 增加 300 秒（5 分钟）
        facilityStartTime.update(Map(key_String -> (lastTimestampOpt, lastTimestampOpts)))
      }

      if (responses.nonEmpty && num) {
        // 更新设备启动时间
        val timestamp = responses.map(_.timestamp).max
        updateFacilityStartTime(faStartime, keynum.toString, timestamp)
        facilityStartTime.update(faStartime + (key_String -> (Some(timestamp), Some(timestamp))))
      }
      // 处理每条响应
      responses.sortBy(_.timestamp).foreach { response =>
        processResponse(out, response, batchData)
      }
    }
  }

  private def getFacilityStartTime(): Map[String, (Option[Long], Option[Long])] = {
    Option(facilityStartTime.value()).getOrElse(Map.empty[String, (Option[Long], Option[Long])])
  }

  private def extractIotFields(batchData: List[KrProtocolData]): String = {
    batchData.flatMap(data => data.iot_field).mkString(",")
  }

  private def extractDeviceNames(batchData: List[KrProtocolData]): String = {
    batchData.flatMap(data => data.iot_table).distinct.mkString(",")
  }

  private def updateFacilityStartTime(faStartime: Map[String, (Option[Long], Option[Long])], device: String, timestamp: Long): Unit = {
    val key_String = s"root.ln.`${device.toString}`"
    facilityStartTime.update(faStartime + (key_String -> (Some(timestamp), Some(timestamp))))
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
                    // 非首次且为WARNING或SERIOUS时跳过
                    if (start_time.isDefined) {
                      // 更新时间
                      val updatedFaultState = fault + (key -> (start_time, Some(response.timestamp), alertLevel))
                      faultStates.update(updatedFaultState)
                    } else {
                      // 首次出现WARNING或SERIOUS时记录时间戳并发送数据
                      val updatedFaultState = fault + (key -> (Some(response.timestamp), None, alertLevel))
                      faultStates.update(updatedFaultState)
                      out.collect(OutputMq(timestamp = response.timestamp, startTime = response.timestamp, currentType = 0, dataType = rudata.data_type.getOrElse(""), deviceId = rudata.device_id.getOrElse(0L), iot_table = iot_table, iot_field = fieldName, alarm_level = alertLevel, endTime = 0L))
                    }
                  } else {
                    // 当alertLevel不为WARNING或SERIOUS时，如果key存在则移除并发送数据
                    if (fault.contains(key)) {
                      if (end_time.nonEmpty) {
                        out.collect(OutputMq(timestamp = response.timestamp, startTime = start_time.getOrElse(0L), endTime = end_time.getOrElse(0L), currentType = 1, dataType = rudata.data_type.getOrElse(""), deviceId = rudata.device_id.getOrElse(0L), iot_table = iot_table, iot_field = fieldName, alarm_level = "ALARM"))
                      }
                      faultStates.update(fault - key)
                    }
                  }
                case None =>
                  // 如果是首次遇到WARNING或SERIOUS，则记录时间戳并发送数据
                  if (alertLevel == "WARNING" || alertLevel == "SERIOUS") {
                    out.collect(OutputMq(timestamp = response.timestamp, startTime = response.timestamp, endTime = 0L, currentType = 0, dataType = rudata.data_type.getOrElse(""), deviceId = rudata.device_id.getOrElse(0L), iot_table = iot_table, iot_field = fieldName, alarm_level = alertLevel))
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
    if (criticalLower.isDefined || criticalUpper.isDefined) {
      val lower = criticalLower.getOrElse(0.0f)
      val upper = criticalUpper.getOrElse(0.0f)
      if (tranvalue < lower || tranvalue > upper) {
        return "SERIOUS"
      }
    }

    // 再检查警告上下限
    val warningLower = rudata.warning_lower_limit
    val warningUpper = rudata.warning_upper_limit
    if (warningLower.isDefined || warningUpper.isDefined) {
      val lower = warningLower.getOrElse(0.0f)
      val upper = warningUpper.getOrElse(0.0f)
      if (tranvalue < lower || tranvalue > upper) {
        return "WARNING"
      }
    }

    // 都未触发则返回正常
    "NORMAL"
  }

  // 将任意类型的值转换为 Option[Double]
  private def toDouble(value: Any): Option[Double] = value match {
    case Some(v) => toDouble(v) // 递归调用 toDouble 处理 Option 内部的值
    case n: Number => Some(n.doubleValue())
    case s: String => Try(s.toDouble).toOption
    case _ =>
      println(s"Failed to convert value to Double: $value")
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
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(15).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(300).toEpochMilli
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case (Some(lastTs), Some(lastTs2)) if lastTs < lastTs2 =>
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${lastTs} AND time < ${lastTs2}"
      case (Some(lastTs), _) =>
        val nextStartTime = Instant.ofEpochMilli(lastTs).plusSeconds(15).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs).plusSeconds(300).toEpochMilli
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case _ =>
        s"SELECT ${iotFlds} FROM ${device} ORDER BY time asc LIMIT 10"
    }
  }
}