package Test

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

object MqttSenderExampleScala {
  // MQTT 代理地址
  private val BROKER_URL = "tcp://10.30.1.125:1883"
  // 客户端 ID
  private val CLIENT_ID = "test-mqtt-sender-scala"
  // MQTT 主题
  private val TOPIC = "pointdata/alarm"
  // 消息质量等级
  private val QOS = 1

  def main(args: Array[String]): Unit = {
    try {
      // 创建 MQTT 客户端实例
      val client = new MqttClient(BROKER_URL, CLIENT_ID, new MemoryPersistence())

      // 创建 MQTT 连接选项
      val connOpts = new MqttConnectOptions()
      connOpts.setCleanSession(true)
      client.connect(connOpts)
      println("Connected")

      // 模拟要发送的消息
      val messagePayload =
        """
          |{
          |  "timestamp": 1632123456,
          |  "startTime": 1632123000,
          |  "endTime": 1632123600,
          |  "currentType": 1,
          |  "dataType": "testType",
          |  "siteId": "123",
          |  "deviceId": 456,
          |  "iotTableName": "table123",
          |  "iotFieldName": "field123",
          |  "alarmLevel": "WARNING"
          |}
        """.stripMargin

      val message = new MqttMessage(messagePayload.getBytes)
      message.setQos(QOS)

      // 发布消息
      println(s"Publishing message: $messagePayload")
      client.publish(TOPIC, message)
      println("Message published")

      // 断开连接
      client.disconnect()
      println("Disconnected")
    } catch {
      case me: MqttException =>
        println(s"reason ${me.getReasonCode}")
        println(s"msg ${me.getMessage}")
        println(s"loc ${me.getLocalizedMessage}")
        println(s"cause ${me.getCause}")
        println(s"excep $me")
        me.printStackTrace()
    }
  }
}