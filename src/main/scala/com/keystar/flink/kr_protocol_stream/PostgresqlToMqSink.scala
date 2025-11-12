package com.keystar.flink.kr_protocol_stream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttConnectOptions, MqttMessage}

import scala.collection.mutable.ListBuffer
import org.eclipse.paho.client.mqttv3._

case class OutputMq(timestamp: Long, startTime: Long, endTime: Long, currentType: Int, deviceId: Long, dataType: String, iot_table: String, iot_field: String, alarm_level: String)

class PostgresqlToMqSink extends RichSinkFunction[OutputMq] {
  private var mqttClient: MqttClient = _
  private val brokerUrl02 = "tcp://10.190.6.97:1883" // MQTT broker 地址
  private val brokerUrl = "tcp://10.190.6.97:54050" // 本地测试 MQTT broker 地址
  private val brokerUrl022 = "tcp://172.16.1.34:1883" // 本地测试 MQTT broker 地址
  private val clientId = "flink-mqtt-sink0515" // 客户端ID
  private val topic = "pointdata/alarm" // MQTT topic
  private val bufferSize = 100 // 缓冲区大小，达到这个数量就批量发送
  private val bufferTimeout = 5000L // 缓冲区超时时间（毫秒），达到这个时间就批量发送
  private var buffer: ListBuffer[OutputMq] = ListBuffer.empty[OutputMq]
  private var lastFlushTime: Long = System.currentTimeMillis()
  private var isReconnecting = false // 重连标志位

  override def open(parameters: Configuration): Unit = {
    try {
      // 创建 MQTT 客户端实例
      mqttClient = new MqttClient(brokerUrl, clientId, new MemoryPersistence())

      // 创建 MQTT 连接选项
      val connOpts = new MqttConnectOptions()
      connOpts.setCleanSession(true)
      connOpts.setAutomaticReconnect(true) // 启用自动重连

      // 连接到 MQTT 代理
      mqttClient.connect(connOpts)
    } catch {
      case me: MqttException =>
        throw me
    }
  }

  override def invoke(value: OutputMq, context: SinkFunction.Context): Unit = {
    if ((value.alarm_level == "WARNING") || (value.alarm_level == "SERIOUS") || (value.endTime!=0)) {
      buffer += value
//      println(s"打印长度${buffer.size}")
      if (buffer.size >= bufferSize) {
        println(s"[Thread ${Thread.currentThread().getId}] 打印长度：${buffer.size}")
//        println(s"[Thread ${Thread.currentThread().getId}] ${System.currentTimeMillis() - lastFlushTime}")
        flushBuffer()
        lastFlushTime = System.currentTimeMillis()
      }else{
        Thread.sleep(2)
        if(buffer.nonEmpty){
          flushBuffer()
        }
      }
    }
  }

  private def flushBuffer(): Unit = {
    val numberPattern = """\d+""".r
    if (buffer.nonEmpty) {
      val messages = buffer.map { value =>
        val message = new MqttMessage()
        val payload = s"""
          {
            "timestamp": ${value.timestamp},
            "startTime": ${value.startTime},
            "endTime": ${value.endTime},
            "currentType": ${value.currentType},
            "dataType": "${value.dataType}",
            "siteId": "${(numberPattern findAllIn value.iot_table).next()}",
            "deviceId": ${value.deviceId},
            "iotTableName": "${value.iot_table}",
            "iotFieldName": "${value.iot_field}",
            "alarmLevel": "${value.alarm_level}"
          }
        """.getBytes("UTF-8")
        message.setPayload(payload)
        message
      }
      if (mqttClient.isConnected) {
        messages.foreach(mqttClient.publish(topic, _))
      } else {
        println("MQTT client is not connected. Attempting to reconnect...")
        mqttClient.reconnect()
        messages.foreach(mqttClient.publish(topic, _))
      }
      buffer.clear()
      lastFlushTime = System.currentTimeMillis()
    }
  }

  override def close(): Unit = {
    try {
      // 关闭前先清空缓冲区
      flushBuffer()
      // 关闭 MQTT 客户端
      if (mqttClient != null && mqttClient.isConnected) {
//        println(s"[Thread ${Thread.currentThread().getId}] Disconnecting from MQTT broker...")
        mqttClient.disconnect()
//        println(s"[Thread ${Thread.currentThread().getId}] Disconnected from MQTT broker")
      }
    } catch {
      case me: MqttException =>
        println(s"[Thread ${Thread.currentThread().getId}] Failed to disconnect from MQTT broker: ${me.getMessage}")
    }
  }

}