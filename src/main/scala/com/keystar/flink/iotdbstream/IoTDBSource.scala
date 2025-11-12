package com.keystar.flink.iotdbstream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.collection.JavaConverters._
import java.util.Base64
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.{HttpResponse, HttpStatus, StatusLine}
import org.apache.http.entity.ContentType
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import play.api.libs.json._

import java.sql.Timestamp
import scala.concurrent.duration.DurationInt

case class IoTDBReading(
                         timestamp: Long,
                         values: Map[String, Option[Any]]
                       )

object IoTDBSource extends RichParallelSourceFunction[IoTDBReading] {
  private var isRunning = true
  private var sourceContext: SourceContext[IoTDBReading] = _

  // IoTDB 的查询 URL 和 SQL 语句
  val iotdb_url2 = "http://192.168.5.13:18080/rest/v2/query"
  val iotdb_url = "http://172.16.1.35:18080/rest/v2/query" //木垒
  val iotdb_url3  = "http://10.190.6.99:18080/rest/v2/query"
  val iotdb_url5 = "http://10.190.6.101:18080/rest/v2/query"  //北京
  val iotdb_url05 = "http://10.190.6.97:54036/rest/v2/query" //本地测高井
  val iotdb_url4 = "http://10.122.1.71:18080/rest/v2/query"  //京津
  val query_statement = "select  I082_9_INVERTER_PhsAB_U,I082_9_INVERTER_PhsBC_U from root.ln.`1269014857442188595`  limit 2"

  // 设置基本认证信息
  val username = "root"
  val password = "root"
  val auth_header = s"Basic ${Base64.getEncoder.encodeToString(s"$username:$password".getBytes)}"

  override def run(ctx: SourceContext[IoTDBReading]): Unit = {
    sourceContext = ctx
    while (isRunning) {
      val result = Try(sendRequest(query_statement))
      result match {
        case Success(response) =>
          println(s"Received response: $response")
          handleResponse(response)
        case Failure(exception) =>
          exception.printStackTrace()
          println(s"Error in sending request: ${exception.getMessage}")
      }
      Thread.sleep(10000) // 模拟定期查询，实际应用中可以调整或移除
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  def sendRequest02(query_statement:String): JsValue = {
    val httpClient = HttpClients.createDefault()
    val httpPost = new HttpPost(iotdb_url)
    httpPost.setHeader("Content-Type", "application/json")
    httpPost.setHeader("Authorization", auth_header)
    val data = Json.obj("sql" -> query_statement)
    httpPost.setEntity(new StringEntity(data.toString()))

    val response = httpClient.execute(httpPost)
    val statusLine: StatusLine = response.getStatusLine
    if (statusLine.getStatusCode == 200) {
      val responseBody = EntityUtils.toString(response.getEntity)
      Json.parse(responseBody)
    } else {
      throw new RuntimeException(s"Failed to execute query: ${statusLine.getReasonPhrase}")
    }
  }

  def sendRequest(query_statement: String): JsValue = {
    // 创建 HttpClient 并设置超时时间
    val requestConfig = RequestConfig.custom()
      .setConnectTimeout(10000) // 连接超时时间（毫秒）
      .setSocketTimeout(10000)  // 读取超时时间（毫秒）
      .build()

    val httpClient = HttpClients.custom()
      .setDefaultRequestConfig(requestConfig)
      .build()

    try {
      // 创建 HTTP POST 请求
      val httpPost = new HttpPost(iotdb_url)
      httpPost.setHeader("Content-Type", "application/json")
      httpPost.setHeader("Authorization", auth_header)

      // 构造请求体
      val data = Json.obj("sql" -> query_statement)
      httpPost.setEntity(new StringEntity(data.toString()))

      // 执行请求
      val response = httpClient.execute(httpPost)
      val statusLine = response.getStatusLine

      if (statusLine.getStatusCode == HttpStatus.SC_OK) {
        val responseBody = EntityUtils.toString(response.getEntity)
        Json.parse(responseBody)
      } else {
        throw new RuntimeException(s"Failed to execute query: ${statusLine.getReasonPhrase}")
      }
    } catch {
      case e: Exception =>
        // 等待一秒后重新调用
        Thread.sleep(1000.milliseconds.toMillis)
        sendRequest(query_statement)
    } finally {
      // 关闭 HttpClient
      httpClient.close()
    }
  }

  def handleResponse(response: JsValue): List[IoTDBReading] = {
    val expressions = (response \ "expressions").asOpt[List[String]].getOrElse(List.empty)
    val timestamps = (response \ "timestamps").asOpt[List[Long]].getOrElse(List.empty)
    val jsValues = (response \ "values").asOpt[List[JsValue]].getOrElse(List.empty)

    // 解析 values 并确保转换正确
    val parsedValues = jsValues.map(_.as[List[JsValue]].map {
      case JsNumber(n) => Some(n.toDouble)
      case JsString(n) => Some(n.toString)
      case JsBoolean(bool) => Some(bool)
      case JsNull => Some(null)
      case _ => null
    })

    val parsedValue=expressions.zip(parsedValues)
    // 创建 IoTDBReading 对象
    val readings = for {
      (timestamp, index) <- timestamps.zipWithIndex
      (expression, values) <- parsedValue
      if index < values.length
    } yield {
      val result: Map[String, Option[Any]] = Map(expression -> values(index))
      IoTDBReading(timestamp, result)
    }
    // 发送读取的数据到 Flink
    readings
  }
}