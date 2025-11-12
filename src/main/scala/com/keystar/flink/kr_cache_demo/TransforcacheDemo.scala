package com.keystar.flink.kr_cache_demo

import com.keystar.flink.iotdbfunction.DiagnosisResult
import com.keystar.flink.iotdbfunction.IotdbFunction.isFalseOrTrue
import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import com.keystar.flink.kr_protocol_stream.{KrProtocolData, OutputMq}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import play.api.libs.json.{JsArray, JsBoolean, JsNull, JsNumber, JsString, JsValue, Json}

import java.net.{HttpURLConnection, URL}
import java.time.Instant
import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.util.Try

class    TransforcacheDemo extends KeyedProcessFunction[String, DiagnosisResult, Unit] {

  private var buffer: ListState[DiagnosisResult] = _
  private var countState: ValueState[Int] = _
  private var lastUpdateTime: ValueState[Long] = _
  private val flushInterval = 1000L // 1秒
  private val batchSize = 1000 // 每批次最大元素数量
  private val idleTimeout =  1000L // 5分钟没有新数据则刷新

  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ListStateDescriptor[DiagnosisResult]("resultState", classOf[DiagnosisResult])
    buffer = getRuntimeContext.getListState(stateDescriptor)
    val countDescriptor = new ValueStateDescriptor[Int]("countState", classOf[Int])
    countState = getRuntimeContext.getState(countDescriptor)
    val timeDescriptor = new ValueStateDescriptor[Long]("lastUpdateTime", classOf[Long])
    lastUpdateTime = getRuntimeContext.getState(timeDescriptor)
  }

  override def processElement(value: DiagnosisResult, ctx: KeyedProcessFunction[String, DiagnosisResult, Unit]#Context, out: Collector[Unit]): Unit = {
    //    println(s"Data received in subtask ${Thread.currentThread().getId}: $value")
    buffer.add(value)

    // 更新计数器和最后更新时间
    val currentCount = Option(countState.value()).getOrElse(0)
    countState.update(currentCount + 1)
    lastUpdateTime.update(ctx.timerService().currentProcessingTime())

    // 如果达到了批次大小，则立即刷新并重置计数器
    if (currentCount >= batchSize - 1) { // 因为已经加了1，所以这里减1
//      flushBatch()
//      countState.clear()
//      lastUpdateTime.clear()
      println("hello")
    }else {
      // 设置定时器以每 flushInterval 秒刷新一次
      val currentTime = ctx.timerService().currentProcessingTime()
      ctx.timerService().registerProcessingTimeTimer(currentTime + flushInterval)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, DiagnosisResult, Unit]#OnTimerContext, out: Collector[Unit]): Unit = {
    val currentTime = ctx.timerService().currentProcessingTime()
    val lastUpdate = Option(lastUpdateTime.value()).getOrElse(0L)

    // 检查是否有超过 idleTimeout 时间没有新数据到达，并且 countState 小于 batchSize
    if ((currentTime - lastUpdate) > idleTimeout && Option(countState.value()).getOrElse(0) < batchSize) {
//      flushBatch()
//      countState.clear() // 定时器触发后也应重置计数器
//      lastUpdateTime.clear()
      println("hello")
    }
  }

  // 定义一个辅助函数来安全地从 ListState 获取数据
  private def getBatchFromBuffer: List[DiagnosisResult] = {
    buffer.get().iterator().asScala.toList
  }

  def flushBatch(): Unit = {
    try {
      val batch = getBatchFromBuffer
      if (batch.nonEmpty) {
        println(s"Flushing batch of size ${batch.size} in subtask ${getRuntimeContext.getIndexOfThisSubtask}.")
        val jsonString = OrganizeToData(batch)
        //        println(s"打印json:$jsonString")
        sendToIoTDB(jsonString)
        println("插入数据了！！！")
        buffer.clear() // 清除 buffer 中的数据
      } else {
        println(s"No data to flush in subtask ${getRuntimeContext.getIndexOfThisSubtask}")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException(s"Failed to write batch to IoTDB for site in subtask ${getRuntimeContext.getIndexOfThisSubtask}: ${e.getMessage}", e)
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

    val siteBatch = data.toArray
    val timestamps = siteBatch.map(_.timestamp).toList
    val dataTypes = siteBatch.map { data =>
      (Option(data.collectedValue), Option(data.validValue)) match {
        case (None, None) => List("TEXT")
        case (Some(_), None) => List("FLOAT", "TEXT")
        case (None, Some(_)) => List("TEXT", "FLOAT")
        case (Some(_), Some(_)) => List("FLOAT", "TEXT", "FLOAT")
      }
    }
    val values = siteBatch.map { data =>
      (Option(data.collectedValue), Option(data.validValue)) match {
        case (None, None) => List(data.faultDescription)
        case (Some(cv), None) => List(cv, data.faultDescription)
        case (None, Some(vv)) => List(data.faultDescription, vv)
        case (Some(cv), Some(vv)) => List(cv, data.faultDescription, vv)
      }
    }
    val isAligned = false
    val device = siteBatch.map { data =>
      val lastDotIndex = data.expressions.lastIndexOf('.')
      data.expressions.substring(0, lastDotIndex)
    }
    val measurements = siteBatch.map { data =>
      val lastDotIndex = data.expressions.lastIndexOf('.')
      val baseName = data.expressions.substring(lastDotIndex + 1)
      (Option(data.collectedValue), Option(data.validValue)) match {
        case (None, None) => List(baseName + "_datasample")
        case (Some(_), None) => List(baseName, baseName + "_datasample")
        case (None, Some(_)) => List(baseName + "_datasample", baseName + "_dataphysics")
        case (Some(_), Some(_)) => List(baseName, baseName + "_datasample", baseName + "_dataphysics")
      }
    }

    val jsonObj = Json.obj(
      "timestamps" -> JsArray(timestamps.map(t => JsNumber(t))),
      "measurements_list" -> JsArray(measurements.map(ms => JsArray(ms.map(JsString.apply)))),
      "data_types_list" -> JsArray(dataTypes.map(ms => JsArray(ms.map(JsString.apply)))),
      "values_list" -> JsArray(values.map(ms => JsArray(ms.map(playJsonValue)))),
      "is_aligned" -> JsBoolean(isAligned),
      "devices" -> JsArray(device.map(JsString.apply))
    )
    Json.stringify(jsonObj)
  }

  private def sendToIoTDB(data: String): Unit = {
    val url = "http://192.168.5.13:18080/rest/v2/insertRecords"
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
}

/*
*
*
docker run -d -p 8081:8081 --name flink-jobmanager --network flink-network --mount type=bind,src=/home/debo/dockerData/flink/jobmanager-conf.yaml,target=/opt/flink/conf/flink-conf.yaml flink:1.16.3-java8 jobmanager
docker run -d --name flink-taskmanager1 --network flink-network --mount type=bind,src=/home/debo/dockerData/flink/taskmanager1-conf.yaml,target=/opt/flink/conf/flink-conf.yaml flink:1.16.3-java8 taskmanager
docker run -d --name flink-taskmanager2 --network flink-network --mount type=bind,src=/home/debo/dockerData/flink/taskmanager2-conf.yaml,target=/opt/flink/conf/flink-conf.yaml flink:1.16.3-java8 taskmanager
docker run -d --name flink-taskmanager3 --network flink-network --mount type=bind,src=/home/debo/dockerData/flink/taskmanager3-conf.yaml,target=/opt/flink/conf/flink-conf.yaml flink:1.16.3-java8 taskmanager

* */