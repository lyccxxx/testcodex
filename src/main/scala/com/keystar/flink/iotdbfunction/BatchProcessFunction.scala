package com.keystar.flink.iotdbfunction

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import play.api.libs.json._

import java.net.{HttpURLConnection, URL}
import scala.collection.JavaConverters.asScalaIteratorConverter

case class DiagnosisResult(
                            timestamp: Long,                // 时刻
                            expressions: String,       // 测点
                            collectedValue: Option[Any], // 采集值
                            faultDescription:String,  //故障值
                            validValue: Option[Any],     // 有效值
                            lastValidTimestamp: Option[Long] // 上一个有效值的时间戳
                          )
class BatchProcessFunction extends KeyedProcessFunction[String, DiagnosisResult, Unit] {

  private var buffer: ListState[DiagnosisResult] = _
  private var countState: ValueState[Int] = _
  private var lastUpdateTime: ValueState[Long] = _
  private val flushInterval = 1000L // 1秒
  private val batchSize = 1000 // 每批次最大元素数量
  private val idleTimeout =  1000L // 5分钟没有新数据则刷新
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ListStateDescriptor[DiagnosisResult]("resultState", classOf[DiagnosisResult])
    buffer = getRuntimeContext.getListState(stateDescriptor)
    val countDescriptor = new ValueStateDescriptor[Int]("countState", classOf[Int])
    countState = getRuntimeContext.getState(countDescriptor)
    val timeDescriptor = new ValueStateDescriptor[Long]("lastUpdateTime", classOf[Long])
    lastUpdateTime = getRuntimeContext.getState(timeDescriptor)
  }

  override def processElement(value: DiagnosisResult, ctx: KeyedProcessFunction[String, DiagnosisResult, Unit]#Context, out: Collector[Unit]): Unit = {
    if(value.collectedValue.exists(_ != null)){
      buffer.add(value)

      // 更新计数器和最后更新时间
      val currentCount = Option(countState.value()).getOrElse(0)
      countState.update(currentCount + 1)
      lastUpdateTime.update(ctx.timerService().currentProcessingTime())

      // 如果达到了批次大小，则立即刷新并重置计数器
      if (currentCount >= batchSize - 1) { // 因为已经加了1，所以这里减1
        flushBatch()
        countState.clear()
        lastUpdateTime.clear()
      }else {
        // 设置定时器以每 flushInterval 秒刷新一次
        val currentTime = ctx.timerService().currentProcessingTime()
        ctx.timerService().registerProcessingTimeTimer(currentTime + flushInterval)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, DiagnosisResult, Unit]#OnTimerContext, out: Collector[Unit]): Unit = {
    val currentTime = ctx.timerService().currentProcessingTime()
    val lastUpdate = Option(lastUpdateTime.value()).getOrElse(0L)

    // 检查是否有超过 idleTimeout 时间没有新数据到达，并且 countState 小于 batchSize
    if ((currentTime - lastUpdate) > idleTimeout && Option(countState.value()).getOrElse(0) < batchSize) {
      flushBatch()
      countState.clear() // 定时器触发后也应重置计数器
      lastUpdateTime.clear()
    }
    Thread.sleep(1)
  }

  // 定义一个辅助函数来安全地从 ListState 获取数据
  private def getBatchFromBuffer: List[DiagnosisResult] = {
    buffer.get().iterator().asScala.toList
  }



  def flushBatch(): Unit = {
    var jsonString: String = null
    var batch: List[DiagnosisResult] = null
    val maxRetries = 3 // 最大重试次数
    val retryDelayMs = 1000 // 初始重试延迟（毫秒）

    try {
      batch = getBatchFromBuffer
      if (batch.isEmpty) {
        println(s"No data to flush in subtask ${getRuntimeContext.getIndexOfThisSubtask}")
        return
      }
      logger.info(s"Flushing batch of size ${batch.size} in subtask ${getRuntimeContext.getIndexOfThisSubtask}.")
      jsonString = OrganizeToData(batch)

      // 执行带重试机制的写入
      var success = false
      var retries = 0

      while (!success && retries < maxRetries) {
        try {
          retries += 1
          success = sendToIoTDB(jsonString)

          if (!success) {
            logger.info(s"写入尝试 $retries 失败，准备重试...")
            if (retries < maxRetries) {
              Thread.sleep(retryDelayMs * retries) // 指数退避策略
            }
          }
        } catch {
          case ex: Exception =>
            println(s"写入异常01: ${ex.getMessage}，尝试 ${retries}/${maxRetries}")
            if (retries < maxRetries) {
              Thread.sleep(retryDelayMs * retries)
            }
        }
      }

      if (success) {
        println(s"批次写入成功，重试 ${retries - 1} 次")
        buffer.clear() // 仅在成功时清空缓冲区
      } else {
        println(s"达到最大重试次数 ($maxRetries) 仍失败")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        // 记录失败数据（可选）
        println(s"无法将批次写入 IoTDB: ${e.getMessage}", e)
    }
  }

  private def playJsonValue(value: Any): JsValue = value match {
    case v: Int => JsNumber(v)
    case v: Float => JsNumber(v)
    case v: Double => JsNumber(v)
    case v: Boolean => JsBoolean(v)
    case v: String => JsString(v)
    case v: Long => JsNumber(v)
    case Some(innerValue) => playJsonValue(innerValue) // 处理 Option 内部的值
    case None => JsNull // 如果是 None，则返回 JsNull
    case null => JsNull
    case other => JsNull // 可以添加更多的类型处理
  }

  def OrganizeToData(data: List[DiagnosisResult]): String = {
    if (data.isEmpty) return ""

    // 将数据转换为数组以便后续操作
    val siteBatch = data.toArray

    // 提取时间戳
    val timestamps = siteBatch.map(_.timestamp).toList

    // 提取设备名称
    val devices = siteBatch.map { data =>
      val lastDotIndex = data.expressions.lastIndexOf('.')
      data.expressions.substring(0, lastDotIndex)
    }

    // 提取测量字段名称（仅保留 _basicerr 和 _valid）
    val measurements = siteBatch.map { data =>
      val lastDotIndex = data.expressions.lastIndexOf('.')
      val baseName = data.expressions.substring(lastDotIndex + 1)
      (Option(data.collectedValue), Option(data.validValue)) match {
        case (None, None) => List(baseName + "_basicerr")
        case (Some(_), None) => List(baseName + "_basicerr")
        case (None, Some(_)) => List(baseName + "_basicerr", baseName + "_valid")
        case (Some(_), Some(_)) => List(baseName + "_basicerr", baseName + "_valid")
      }
    }

    // 提取数据类型（与测量字段对应）
    val dataTypes = siteBatch.map { data =>
      (Option(data.collectedValue), Option(data.validValue)) match {
        case (None, None) => List("TEXT")
        case (Some(_), None) => List("TEXT")
        case (None, Some(_)) => List("TEXT", "FLOAT")
        case (Some(_), Some(_)) => List("TEXT", "FLOAT")
      }
    }

    // 提取值（保留 faultDescription 和 validValue）
    val values = siteBatch.map { data =>
      (Option(data.collectedValue), Option(data.validValue)) match {
        case (None, None) => List(data.faultDescription)
        case (Some(_), None) => List(data.faultDescription)
        case (None, Some(vv)) => List(data.faultDescription, vv)
        case (Some(_), Some(vv)) => List(data.faultDescription, vv)
      }
    }

    // 是否对齐
    val site_id = siteBatch.map { data =>
      // 按照 "." 分割字符串
      val parts = data.expressions.split("\\.")

      // 去掉最后一部分
      val resultParts = parts.dropRight(1)

      // 将剩余部分用 "." 拼接起来
      resultParts.mkString(".")
    }.distinct
    // 按照 "." 分割字符串


    var isAligned = false


    for(i <- site_id){
      if(i=="root.ln.`1521714652626283467`"&&site_id.length==1) isAligned=true
    }
    // 构建 JSON 对象
    val jsonObj = Json.obj(
      "timestamps" -> JsArray(timestamps.map(t => JsNumber(t))),
      "measurements_list" -> JsArray(measurements.map(ms => JsArray(ms.map(JsString.apply)))),
      "data_types_list" -> JsArray(dataTypes.map(ms => JsArray(ms.map(JsString.apply)))),
      "values_list" -> JsArray(values.map(ms => JsArray(ms.map(playJsonValue)))),
      "is_aligned" -> JsBoolean(isAligned),
      "devices" -> JsArray(devices.map(JsString.apply))
    )

    // 返回 JSON 字符串
    Json.stringify(jsonObj)
  }




  def sendToIoTDB02(data: String): Unit = {
    val url = "http://172.16.1.35:18080/rest/v2/insertRecords"
    val connection = (new URL(url)).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setRequestProperty("Authorization", "Basic cm9vdDpyb290")
    connection.setDoOutput(true)

    val os = connection.getOutputStream
    os.write(data.getBytes("UTF-8"))
    os.flush()
    os.close()

    val responseCode = connection.getResponseCode
    if (responseCode == 200) {
      println(s"Inserted data successfully with response code: $responseCode")
    } else {
      throw new RuntimeException(s"Failed to insert batch, response code: $responseCode")
    }
  }

  import java.io.{BufferedReader, InputStreamReader}
  import java.net.{HttpURLConnection, URL}

  def sendToIoTDB(data: String): Boolean = {
    val url = "http://172.16.1.35:18080/rest/v2/insertRecords"
    var connection: HttpURLConnection = null

    try {
      connection = (new URL(url)).openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setRequestProperty("Authorization", "Basic cm9vdDpyb290")
      connection.setDoOutput(true)

      // 发送请求体
      val os = connection.getOutputStream
      try {
        os.write(data.getBytes("UTF-8"))
      } finally {
        if (os != null) os.close()
      }

      // 获取响应状态码
      val responseCode = connection.getResponseCode

      if (responseCode == 200) {
        // 读取成功响应内容
        val reader = new BufferedReader(new InputStreamReader(connection.getInputStream))
        try {
          val response = reader.lines().iterator().asScala.mkString("\n")

          // 解析响应内容
          if (response.contains("\"message\":\"SUCCESS_STATUS\"")) {
            logger.info(s"数据成功写入01: $response")
            true
          } else {
            logger.info(s"请求成功但写入可能失败01: $response")
            false
          }
        } finally {
          if (reader != null) reader.close()
        }
      } else {
        // 读取错误响应内容
        val errorReader = new BufferedReader(new InputStreamReader(connection.getErrorStream))
        try {
          val errorResponse = errorReader.lines().iterator().asScala.mkString("\n")
          println(s"写入失败，状态码: $responseCode, 错误信息: $errorResponse")
          false
        } finally {
          if (errorReader != null) errorReader.close()
        }
      }
    } catch {
      case e: Exception =>
        println(s"写入异常01: ${e.getMessage}")
        false
    } finally {
      // 确保连接关闭
      if (connection != null) {
        connection.disconnect()
      }
    }
  }
}