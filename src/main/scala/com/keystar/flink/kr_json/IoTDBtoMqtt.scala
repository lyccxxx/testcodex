package com.keystar.flink.kr_json

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import scala.collection.concurrent.TrieMap
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters.asScalaBufferConverter


class IoTDBtoMqtt extends RichSinkFunction[OutResult] {

  private var mqttClient: MqttClient = _
  private val brokerUrl = "tcp://172.16.1.34:1883" // MQTT服务器地址
  private val clientId = "flink-mqtt-sink2523"        // 唯一客户端ID（确保集群内唯一）
  private val topic = "pointdata/algAlarm"           // MQTT主题
  private val batchSize = 50                       // 批量发送大小
  private val flushIntervalMs = 5000                // 批量发送时间间隔（毫秒）

  // 线程安全的消息缓冲区
  private val messageBuffer = new ConcurrentLinkedQueue[OutResult]()
  // 告警状态存储（键：station.expressions，值：(开始时间, 结束时间)）
  private val alarmStates: TrieMap[String, (Option[Long], Option[Long])] = TrieMap.empty

  // 控制后台线程运行状态（volatile确保可见性）
  @volatile private var isRunning = true

  override def open(parameters: Configuration): Unit = {
    // 配置MQTT连接选项
    val connOpts = new MqttConnectOptions()
    connOpts.setCleanSession(true)                // 清除会话（根据需求调整）
    connOpts.setAutomaticReconnect(true)          // 启用自动重连（关键：处理网络波动）
    connOpts.setKeepAliveInterval(30)             // 30秒心跳检测，维持连接
    connOpts.setConnectionTimeout(10)             // 10秒连接超时

    // 如果MQTT服务器需要认证（取消注释并填入你的凭证）
    // connOpts.setUserName("your-username")
    // connOpts.setPassword("your-password".toCharArray)

    try {
      // 初始化MQTT客户端
      mqttClient = new MqttClient(brokerUrl, clientId, new MemoryPersistence())

      // 设置连接状态回调（处理连接恢复时的消息补发）
      mqttClient.setCallback(new MqttCallbackExtended {
        override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
          logInfo(s"MQTT连接恢复（reconnect=$reconnect）")
          flushBuffer() // 连接恢复后立即发送缓冲消息
        }

        override def connectionLost(cause: Throwable): Unit = {
          logError("MQTT连接丢失，自动重连中...", cause)
        }

        // 以下回调无需实现（本Sink不接收消息）
        override def messageArrived(topic: String, message: MqttMessage): Unit = {}
        override def deliveryComplete(token: IMqttDeliveryToken): Unit = {}
      })

      // 连接到MQTT服务器
      mqttClient.connect(connOpts)
      logInfo("成功连接到MQTT服务器")

      // 启动独立的批量发送线程（使用Lambda表达式避免线程安全问题）
      new Thread(() => {
        while (isRunning) {
          try {
            // 触发批量发送条件：达到批量大小 或 时间间隔到达
            if (messageBuffer.size() >= batchSize || System.currentTimeMillis() - lastFlushTime >= flushIntervalMs) {
              flushBuffer()
            }
            Thread.sleep(100) // 降低CPU占用，控制检测频率
          } catch {
            case e: InterruptedException =>
              // 处理线程中断（如Flink任务取消）
              logInfo("批量发送线程被中断")
              Thread.currentThread().interrupt()
          }
        }
      }).start()

    } catch {
      case ex: Exception =>
        logError("MQTT客户端初始化失败", ex)
        throw ex // 抛出异常让Flink重试重启
    }
  }

  override def invoke(value: OutResult, context: SinkFunction.Context): Unit = {
    if (value != null && value.alarm && isNumericValue(value)) {
      // 检查collectedValue是否为1、2、3中的一个
      value.collectedValue.foreach { v =>
        if (v.isInstanceOf[Double]) {
          val intValue = v.asInstanceOf[Double]
          if (intValue >= 0 && intValue <= 3) {
            messageBuffer.add(value)
          }
        }
      }
    }
  }

  private var lastFlushTime = System.currentTimeMillis()

  private def flushBuffer(): Unit = {
    // 首先检查是否有消息
    if (messageBuffer.isEmpty) return

    // 使用优先级队列进行排序（小顶堆，按时间戳排序）
    val priorityQueue = new java.util.PriorityQueue[OutResult](
      (a: OutResult, b: OutResult) => a.timestamp.compareTo(b.timestamp)
    )

    // 从消息缓冲区取出所有消息放入优先级队列
    var element: OutResult = messageBuffer.poll()
    while (element != null) {
      priorityQueue.add(element)
      element = messageBuffer.poll()
    }

    // 分批处理排序后的消息
    var batchCount = 0
    val batch = new java.util.ArrayList[OutResult](batchSize)

    while (!priorityQueue.isEmpty) {
      // 填充当前批次
      while (!priorityQueue.isEmpty && batch.size() < batchSize) {
        batch.add(priorityQueue.poll())
      }

      // 处理当前批次
      if (!batch.isEmpty) {
        try {
          if (!mqttClient.isConnected) {
            logInfo("等待MQTT连接恢复...")
            // 将当前批次和队列中剩余的消息放回原队列
            batch.forEach(messageBuffer.add)
            while (!priorityQueue.isEmpty) {
              messageBuffer.add(priorityQueue.poll())
            }
            return
          }

          // 处理当前批次的消息
          batch.forEach(processMessage)

          lastFlushTime = System.currentTimeMillis()
          logInfo(s"批量发送完成：${batch.size}条消息")
          batchCount += 1

        } catch {
          case ex: Exception =>
            // 发送失败时，将当前批次和队列中剩余的消息放回原队列
            println(ex)
            batch.forEach(messageBuffer.add)
            while (!priorityQueue.isEmpty) {
              messageBuffer.add(priorityQueue.poll())
            }
            logError(s"批量发送失败：${ex.getMessage}", ex)
            return
        } finally {
          // 清空当前批次，准备下一批
          batch.clear()
        }
      }
    }

    // 处理可能剩余在优先级队列中的消息（如果超过最大批次限制）
    while (!priorityQueue.isEmpty) {
      messageBuffer.add(priorityQueue.poll())
    }
  }

  private def processMessage(value: OutResult): Unit = {

    val key =if(value.expressions.isEmpty)s"${value.station}.${value.properties}_${value.alg_label_EN}"
              else s"${value.station}.${value.expressions}"
//    println(s"打印进来的key值：${value.timestamp} ${key} 值：${value.collectedValue}")
    val currentState = alarmStates.getOrElse(key, (None, None))

    // 直接处理 Option[Boolean]，移除多余的 foreach
    val num = extractAlarmStatus(value)
//    println(s"打印num数值：${num}")
    extractAlarmStatus(value) match {
      case Some(true)  => handleAlarmStart(key, value, currentState)
      case Some(false) => handleAlarmEnd(key, value, currentState)
      case None        => None  // 无效值，直接跳过
    }
  }

  private def extractAlarmStatus(outResult: OutResult): Option[Boolean] = {
    outResult.collectedValue.headOption.flatMap {
      case value: Boolean => Some(value)
      case value: Number =>
        value.doubleValue() match {
          case v if v > 0 => Some(true)  // 任何正数都视为报警开始
          case 0          => Some(false) // 0 视为报警结束
          case _          => None        // 负数或其他数值视为无效
        }
      case value: String =>
        value.toLowerCase match {
          case "true" | "1" | "on"  => Some(true)
          case "false" | "0" | "off" => Some(false)
          case _                    => None
        }
      case _ => None
    }
  }

  private def handleAlarmStart(
                                key: String,
                                value: OutResult,
                                currentState: (Option[Long], Option[Long])
                              ): Unit = {
//    println(s"進入到裏邊的數值：${key} ${currentState}")
    if (currentState._1.isEmpty) {
      // 新告警开始，记录开始时间并发送消息
      alarmStates.put(key, (Some(value.timestamp), None))
      sendMessage(value, Some(value.timestamp), None)
    } else if (currentState._2.isDefined) {
      // 已结束的告警重新触发，更新开始时间
      alarmStates.put(key, (Some(value.timestamp), None))
      sendMessage(value, Some(value.timestamp), None)
    }
  }

  private def handleAlarmEnd(
                              key: String,
                              value: OutResult,
                              currentState: (Option[Long], Option[Long])
                            ): Unit = {
    if (currentState._1.isDefined && currentState._2.isEmpty) {
      // 告警结束，记录结束时间并发送消息，然后清除状态
      val endTime = Some(value.timestamp)
      alarmStates.put(key, (currentState._1, endTime))
      sendMessage(value, currentState._1, endTime)
      alarmStates.remove(key) // 清除已结束的告警状态
    }
  }

  private def sendMessage(
                           value: OutResult,
                           startTime: Option[Long],
                           endTime: Option[Long]
                         ): Unit = {
    // 修正时间顺序：确保startTime <= endTime
//    println(s"打印数值发送数值key：${value.timestamp} ${value.station}_${value.properties}  值：${value.collectedValue}")
    val payload = createJsonPayload(value, startTime, endTime)
    val outputJson = JsonConverter.convertToJson(payload)
    val message = new MqttMessage(outputJson.getBytes(UTF_8))
    message.setQos(1) // 至少一次交付，保证消息不丢失（可能重复）
    mqttClient.publish(topic, message)
  }
  import play.api.libs.json._
  private def createJsonPayload(
                                 value: OutResult,
                                 startTimeOpt: Option[Long],
                                 endTimeOpt: Option[Long]
                               ): String = {

    val alg_desc=escapeJsonString(value.alg_desc)
    val level: String = value.collectedValue match {
      case Some(v) => v match {
        case d: Double => d match {
          case 1.0 => "warning"
          case 2.0 | 3.0 => "serious"
          case _ => "alarm"
        }
        case s: String =>
          try {
            s.toDouble match {
              case 1.0 => "warning"
              case 2.0 | 3.0 => "serious"
              case _ => "alarm"
            }
          } catch {
            case _: NumberFormatException => "alarm"
          }
        case _ => "alarm" // 非Double或String类型
      }
      case None => "alarm" // 无值的情况
    }
    val inputString = value.station

    // 使用indexOf和lastIndexOf找到反引号的位置
    val startIndex = inputString.indexOf('`') + 1
    val endIndex = inputString.lastIndexOf('`')
    val site_id = inputString.substring(startIndex, endIndex)
    val express = if (value.expressions != null && value.expressions.nonEmpty) {
      value.expressions
    } else {
      // 处理 properties 和 alg_label_EN 可能为 null 的情况
      val prop = Option(value.properties).getOrElse("")
      val label = Option(value.alg_label_EN).getOrElse("")

      // 避免前导/尾随下划线
      if (prop.isEmpty) label else s"${prop}_${label}"
    }
    s"""
       |{
       |  "station": "${value.station}",
       |  "siteId":"${site_id}",
       |  "start_time": ${startTimeOpt.getOrElse("null")},
       |  "end_time": ${endTimeOpt.getOrElse(0L)},
       |  "expressions": "${express}",
       |  "collectedValue": "${value.collectedValue.getOrElse("null")}",
       |  "level": "${level}",
       |  "properties": "${value.properties}",
       |  "alg_desc": "${alg_desc}",
       |  "alg_label_EN":"${value.alg_label_EN}",
       |  "alg_brief_CH":"${value.alg_brief_CH}",
       |  "alg_param":"${value.alg_param}",
       |  "alg_show_group":"${value.alg_show_group}",
       |  "alg_show_levelcursor":"${value.alg_show_levelcursor}",
       |  "timestamp": ${value.timestamp}
       |}
     """.stripMargin.replaceAll("\n\\s+", " ") // 压缩JSON格式，去除换行和多余空格
  }

  def escapeJsonString(input: String): String = {
    input.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
  }

  // 判断collectedValue是否为布尔值
  private def isBooleanValue(outResult: OutResult): Boolean = {
    outResult.collectedValue.exists(_.isInstanceOf[Boolean])
  }

  /**
   * 判断 OutResult 中的 collectedValue 是否包含数值类型
   * 支持的数值类型包括：Int, Long, Float, Double, BigDecimal, Byte, Short
   */
  private def isNumericValue(outResult: OutResult): Boolean = {
    outResult.collectedValue.exists { value =>
      value match {
        case _: Int    => true
        case _: Long   => true
        case _: Float  => true
        case _: Double => true
        case _: BigDecimal => true
        case _: Byte   => true
        case _: Short  => true
        case _         => false
      }
    }
  }

  // 提取布尔值（安全类型转换）
  private def extractBooleanValue(outResult: OutResult): Option[Boolean] = {
    outResult.collectedValue match {
      case Some(value: Boolean) => Some(value)
      case _ => None
    }
  }

  // 日志输出方法（生产环境建议使用Flink内置日志）
  private def logInfo(message: String): Unit = {
    println(s"[INFO] [${Thread.currentThread().getName}] $message")
  }

  private def logError(message: String, ex: Throwable): Unit = {
    println(s"[ERROR] [${Thread.currentThread().getName}] $message: ${ex.getMessage}")
    ex.printStackTrace()
  }

  override def close(): Unit = {
    // 1. 停止后台发送线程
    isRunning = false

    // 2. 发送剩余的缓冲消息（确保任务关闭前数据发送完毕）
    flushBuffer()

    // 3. 关闭MQTT连接
    if (mqttClient != null && mqttClient.isConnected) {
      mqttClient.disconnect()
      logInfo("MQTT客户端已安全断开连接")
    }
  }
}