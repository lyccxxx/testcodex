package com.keystar.flink.kr_json
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import play.api.libs.json._

import java.net.{HttpURLConnection, URL}
import scala.collection.JavaConverters.asScalaIteratorConverter

case class OutResult(
                            timestamp: Long,                // 时刻
                            expressions: String,       // 测点
                            collectedValue: Option[Any], // 计算的布尔值
                            alarm:Boolean,
                            level:String,
                            station:String, //站点
                            properties:String,
                            alg_desc: String,              // 保留原字段
                            alg_label_EN: String,          // 新增字段
                            alg_brief_CH: String,          // 新增字段
                            alg_param: Map[String, List[Double]], // 使用Map匹配JSON结构
                            alg_show_group: List[Group], // 直接使用Map匹配JSON对象
                            alg_show_levelcursor: List[String]
                          )

class IoTDBNumerSink extends KeyedProcessFunction[String, OutResult, Unit] {

  private var buffer: ListState[OutResult] = _
  private var countState: ValueState[Int] = _
  private var lastUpdateTime: ValueState[Long] = _
  private val flushInterval = 1000L // 1秒
  private val batchSize = 1200 // 每批次最大元素数量
  private val idleTimeout =  1000L // 5分钟没有新数据则刷新
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ListStateDescriptor[OutResult]("resultState", classOf[OutResult])
    buffer = getRuntimeContext.getListState(stateDescriptor)
    val countDescriptor = new ValueStateDescriptor[Int]("countState", classOf[Int])
    countState = getRuntimeContext.getState(countDescriptor)
    val timeDescriptor = new ValueStateDescriptor[Long]("lastUpdateTime", classOf[Long])
    lastUpdateTime = getRuntimeContext.getState(timeDescriptor)
  }

  override def processElement(value: OutResult, ctx: KeyedProcessFunction[String, OutResult, Unit]#Context, out: Collector[Unit]): Unit = {
    //    println(s"Data received in subtask ${Thread.currentThread().getId}: $value")
    if(value.collectedValue.exists(_!=null)){
      buffer.add(value)
    }

    // 更新计数器和最后更新时间
    val currentCount = Option(countState.value()).getOrElse(0)
    countState.update(currentCount + 1)
    lastUpdateTime.update(ctx.timerService().currentProcessingTime())

    // 如果达到了批次大小，则立即刷新并重置计数器
    if (currentCount >= batchSize - 1) { // 因为已经加了1，所以这里减1
//      println(s"输出长度：${currentCount}")
      flushBatch()
      countState.clear()
      lastUpdateTime.clear()
    }else {
      // 设置定时器以每 flushInterval 秒刷新一次
      val currentTime = ctx.timerService().currentProcessingTime()
      ctx.timerService().registerProcessingTimeTimer(currentTime + flushInterval)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OutResult, Unit]#OnTimerContext, out: Collector[Unit]): Unit = {
    val currentTime = ctx.timerService().currentProcessingTime()
    val lastUpdate = Option(lastUpdateTime.value()).getOrElse(0L)

    // 检查是否有超过 idleTimeout 时间没有新数据到达，并且 countState 小于 batchSize
    if ((currentTime - lastUpdate) > idleTimeout && Option(countState.value()).getOrElse(0) < batchSize) {
      flushBatch()
      countState.clear() // 定时器触发后也应重置计数器
      lastUpdateTime.clear()
      import org.slf4j.LoggerFactory
      val logger = LoggerFactory.getLogger(this.getClass)
//      logger.info(s"Processed data 数据结束时间")
    }
  }

  // 定义一个辅助函数来安全地从 ListState 获取数据
  private def getBatchFromBuffer: List[OutResult] = {
    buffer.get().iterator().asScala.toList
  }

  def flushBatch(): Unit = {
    var jsonString: String = null
    var batch: List[OutResult] = null
    val maxRetries = 3 // 最大重试次数

    try {
      batch = getBatchFromBuffer
      if (batch.isEmpty) {
        println(s"No data to flush in subtask ${getRuntimeContext.getIndexOfThisSubtask}")
        return
      }
      logger.info(s"Flushing batch of size ${batch.size} in subtask ${getRuntimeContext.getIndexOfThisSubtask}.")
      jsonString = OrganizeToData(batch)

      // 执行重试逻辑
      var success = false
      var retries = 0

      while (!success && retries < maxRetries) {
        retries += 1
        success = sendToIoTDB(jsonString)

        if (!success) {
          logger.info(s"Insertion attempt $retries failed. Retrying...")
          // 可选：添加指数退避策略，避免立即重试
          if (retries < maxRetries) {
            Thread.sleep(1000 * retries) // 简单退避：1秒、2秒、3秒...
          }
        }
      }

      if (success) {
        println(s"Successfully inserted after $retries attempts.")
        buffer.clear() // 仅在最终成功时清空缓冲区
      } else {
        println(s"Failed to insert after $maxRetries attempts.")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        // 可以考虑将失败数据落盘或记录日志用于后续处理
        println(s"Failed to write batch to IoTDB for site in subtask ${getRuntimeContext.getIndexOfThisSubtask}: ${e.getMessage}", e)
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

  def OrganizeToData(data: List[OutResult]): String = {
    if (data.isEmpty) return ""

    val siteBatch = data.toArray

    // 时间戳列表
    val timestamps = siteBatch.map(_.timestamp).toList

    // 数据类型列表
    val dataTypes = siteBatch.map { data =>
      data.collectedValue match {
        case Some(value) =>
          value match {
            case _: Int | _: Double | _: Float => List("FLOAT")
            case _: Boolean => List("BOOLEAN")
            case _: String => List("TEXT")
            case _ => List("TEXT") // 默认情况或其他类型
          }
        case None => List("TEXT")
      }
    }

    // 值列表
    val values = siteBatch.map { data =>
      data.collectedValue match {
        case Some(cv) => List(cv)
        case None => List(0)
      }
    }

    var isAligned = false
    val isAligned02 = false

    val site_id = siteBatch.map { data =>
      // 按照 "." 分割字符串
      val parts = data.station
      parts
    }.distinct

    for(i <- site_id){
      if(i=="root.ln.`1521714652626283467`"&&site_id.length==1) isAligned=true
    }
    // 设备列表
    val device = siteBatch.map { data =>
      data.station
    }

    // 测量列表
    val measurements = siteBatch.map { data =>
      val baseName = data.expressions
      data.collectedValue match {
        case Some(_) => List(baseName)
        case None => List(baseName + "_valid")
      }
    }

    // 构建JSON对象
    val jsonObj = Json.obj(
      "timestamps" -> JsArray(timestamps.map(t => JsNumber(BigDecimal(t)))),
      "measurements_list" -> JsArray(measurements.map(ms => JsArray(ms.map(JsString.apply)))),
      "data_types_list" -> JsArray(dataTypes.map(ms => JsArray(ms.map(JsString.apply)))),
      "values_list" -> JsArray(values.map { valueList =>
        JsArray(valueList.map {
          case v: String => JsString(v)
          case v: Boolean => JsBoolean(v)
          case v: Number => JsNumber(BigDecimal(v.toString))
          case _ => JsNull
        })
      }),
      "is_aligned" -> JsBoolean(isAligned),
      "devices" -> JsArray(device.map(JsString.apply))
    )

    Json.stringify(jsonObj)
  }

  private def sendToIoTDB01(data: String): Unit = {
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
  import scala.util.parsing.json.JSON

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
            logger.info(s"数据成功写入02: $response")
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
        println(s"写入异常02: ${e.getMessage}")
        false
    } finally {
      // 确保连接关闭
      if (connection != null) {
        connection.disconnect()
      }
    }
  }
}