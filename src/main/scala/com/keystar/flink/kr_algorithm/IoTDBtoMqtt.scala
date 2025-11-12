package com.keystar.flink.kr_algorithm

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kafka.connect.json.JsonConverter
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.concurrent.TrieMap

// 定义Flink Sink类，继承RichSinkFunction，处理AlgorithResult类型数据
class IoTDBtoMqtt extends RichSinkFunction[AlgorithResult] {

  private var mqttClient: MqttClient = _
  private val brokerUrl = "tcp://172.16.1.34:1883" // MQTT服务器地址
  private val clientId = "flink-mqtt-sink9117"        // 唯一客户端ID（确保集群内唯一）
  private val topic = "pointdata/algAlarm017"           // MQTT主题
  private val batchSize = 100                       // 批量发送大小
  private val flushIntervalMs = 5000                // 批量发送时间间隔（毫秒）

  // 线程安全的消息缓冲区（用于暂存待发送的消息）
  private val messageBuffer = new ConcurrentLinkedQueue[AlgorithResult]()
  // 告警状态存储（键：station.expressions，值：(开始时间, 结束时间)）
  private val alarmStates: TrieMap[String, (Option[Long], Option[Long])] = TrieMap.empty

  // 控制后台线程运行状态（volatile确保可见性）
  @volatile private var isRunning = true


  // Flink生命周期方法：初始化时调用
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
        // 连接完成时调用（包括重连）
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
              Thread.currentThread().interrupt()   // 保留中断状态
          }
        }
      }).start()

    } catch {
      case ex: Exception =>
        logError("MQTT客户端初始化失败", ex)
        throw ex // 抛出异常让Flink重试重启
    }
  }


  //   Flink核心方法：处理每条输入数据
  override def invoke(value: AlgorithResult, context: SinkFunction.Context): Unit = {
    // 过滤条件：数据非空、是告警、collectedValue为数值类型
    if (value != null && value.alarm && isNumericValue(value)) {
      // 检查collectedValue是否为1、2、3中的一个
      value.collectedValue.foreach {
        // 仅处理Number类型（所有数值类型的父类）
        case num: Number =>
          val numericValue = num.doubleValue()  // 统一转为Double，明确类型
          if (numericValue >= 0 && numericValue <= 3) {  // 筛选出0-3范围的数值
            messageBuffer.add(value)  // 加入缓冲区
          }
        // 非数值类型（包括Boolean、String等）直接忽略
        case _ => // 不做处理
      }
    }
  }

  // 记录上次发送时间
  private var lastFlushTime = System.currentTimeMillis()

  // 发送缓冲中的消息
  private def flushBuffer(): Unit = {
    // 首先检查是否有消息:无消息直接返回
    if (messageBuffer.isEmpty) return

        // 使用优先级队列进行排序（小顶堆，按时间戳排序）
        val priorityQueue = new java.util.PriorityQueue[AlgorithResult](
          (a: AlgorithResult, b: AlgorithResult) => a.timestamp.compareTo(b.timestamp)
        )

        // 从消息缓冲区取出所有消息放入优先级队列
        var element: AlgorithResult = messageBuffer.poll()
        while (element != null) {
          priorityQueue.add(element)
          element = messageBuffer.poll()
        }

//    // 修改：对消息进入优先级队列进行二次排序
//    // 修改：1. 从缓冲区取出所有消息
//    val bufferList = new java.util.ArrayList[AlgorithResult]()
//    var element: AlgorithResult = messageBuffer.poll()
//    while (element != null) {
//      bufferList.add(element)
//      element = messageBuffer.poll()
//    }
//
//    // 修改2. 对取出的消息进行第一次排序（转为Scala集合进行排序）
//    import scala.collection.JavaConverters._
//
//    val sortedList = bufferList.asScala.sortBy(_.timestamp).asJava
//
//    // 3. 使用优先级队列进行第二次排序（小顶堆，按时间戳排序）
//    val priorityQueue = new java.util.PriorityQueue[AlgorithResult](
//      (a: AlgorithResult, b: AlgorithResult) => a.timestamp.compareTo(b.timestamp)
//    )
//    // 将第一次排序后的结果加入优先级队列进行二次排序
//    priorityQueue.addAll(sortedList)


    // 新增调试用：筛选并打印expressions为"C6_19_isActPowerLow"的消息
    logInfo(s"===== 开始检查优先级队列中的C6_19_isActPowerLow消息 =====")
    val tempList = new java.util.ArrayList[AlgorithResult]()
    var matchedCount = 0  // 记录符合条件的消息数量
    while (!priorityQueue.isEmpty) {
      val item = priorityQueue.poll()
      // 新增调试用：判断是否为目标表达式消息
      if (item.expressions == "C6_19_isActPowerLow") {
        matchedCount += 1
        // 新增调试用：打印关键信息：时间戳、站点、数值、告警状态
        logInfo(
          s"[C6_19匹配消息] 时间戳: ${item.timestamp}, " +
            s"站点: ${item.station}, " +
            s"数值: ${item.collectedValue}, " +
            s"告警状态: ${extractAlarmStatus(item)}, " +
            s"alarm字段值: ${item.alarm}"
        )
      }
      // 新增调试用：
      tempList.add(item)  // 暂存元素，后续放回队列
    }
    // 新增调试用：打印统计信息
    logInfo(s"===== 优先级队列检查完成，共发现${matchedCount}条C6_19_isActPowerLow消息 =====")

    // 新增调试用：将元素放回队列，保证后续处理正常执行
    priorityQueue.addAll(tempList)


    // 分批处理排序后的消息
    var batchCount = 0
    val batch = new java.util.ArrayList[AlgorithResult](batchSize)

    while (!priorityQueue.isEmpty) {
      // 填充当前批次（不超过batchSize）
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

  // 处理单条消息（判断告警状态并更新）
  private def processMessage(value: AlgorithResult): Unit = {

    // 生成key：station + (expressions为空则省略)
    val key =if(value.expressions.isEmpty)s"${value.station}"
    else s"${value.station}.${value.expressions}"


    //    println(s"打印进来的key值：${value.timestamp} ${key} 值：${value.collectedValue}")

    // 获取当前告警状态
    val currentState = alarmStates.getOrElse(key, (None, None))

    // 提取告警状态（true=开始，false=结束）
    // 直接处理 Option[Boolean]，移除多余的 foreach
    val num = extractAlarmStatus(value)
    extractAlarmStatus(value) match {
      case Some(true)  => handleAlarmStart(key, value, currentState)     // 处理告警开始
      case Some(false) => handleAlarmEnd(key, value, currentState)       // 处理告警开始
      case None        => None  // 无效值，直接跳过
    }
  }

  // 提取告警状态（转换不同类型为布尔值）
  private def extractAlarmStatus(AlgorithResult: AlgorithResult): Option[Boolean] = {
    AlgorithResult.collectedValue.headOption.flatMap {// 处理Option中的值
      // 情况一：布尔值直接使用
      case value: Boolean => Some(value)
      // 情况二：数字类型进行判断：所有正数开始报警，0结束报警，其他数字无效
      case value: Number =>
        value.doubleValue() match {
          case v if v > 0 => Some(true)  // 任何正数都视为报警开始
          case 0          => Some(false) // 0 视为报警结束
          case _          => None        // 负数或其他数值视为无效
        }
      //情况三：字符串类型进行判断："true"、"1"、"on"为开始，"false"、"0"、"off"为结束，其他字符串无效
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
                                value: AlgorithResult,
                                currentState: (Option[Long], Option[Long])
                              ): Unit = {
    //    println(s"進入到裏邊的數值：${key} ${currentState}")
    // 无开始时间（新告警）
    if (currentState._1.isEmpty) {
      // 新告警开始，记录开始时间并发送消息
      alarmStates.put(key, (Some(value.timestamp), None))   // 记录开始时间
      sendMessage(value, Some(value.timestamp), None)       // 发送告警开始消息
    } else if (currentState._2.isDefined) {    // 已结束的告警重新触发
      // 已结束的告警重新触发，更新开始时间
      alarmStates.put(key, (Some(value.timestamp), None))   // 更新开始时间
      sendMessage(value, Some(value.timestamp), None)      // 发送告警开始消息
    }
  }

  private def handleAlarmEnd(
                              key: String,
                              value: AlgorithResult,
                              currentState: (Option[Long], Option[Long])
                            ): Unit = {

    if (currentState._1.isDefined && currentState._2.isEmpty) {
      // 告警结束，记录结束时间并发送消息，然后清除状态
      val endTime = Some(value.timestamp)      // 记录结束时间
      alarmStates.put(key, (currentState._1, endTime))    // 更新状态
      sendMessage(value, currentState._1, endTime)        // 发送告警结束消息
      alarmStates.remove(key) // 清除已结束的告警状态
    }
  }

  // 发送MQTT消息
  private def sendMessage(
                           value: AlgorithResult,
                           startTime: Option[Long],
                           endTime: Option[Long]
                         ): Unit = {
    // 修正时间顺序：确保startTime <= endTime
    //    println(s"打印数值发送数值key：${value.timestamp} ${value.station}_${value.properties}  值：${value.collectedValue}")
    val payload = createJsonPayload(value, startTime, endTime)    // 创建JSON payload
    val outputJson = JsonConverter.convertToJson(payload)         // 转换为JSON
    val message = new MqttMessage(outputJson.getBytes(UTF_8))     // 创建MQTT消息
    message.setQos(1) // 至少一次交付，保证消息不丢失（可能重复）
    //  mqttClient.publish(topic, message)    // 发布消息
    //    println(s"打印发送的MQTT消息：${outputJson}")
    if(value.expressions=="C6_19_isActPowerLow"){
      println(s"打印发送的MQTT消息：${outputJson}"+s"C6_19_isActPowerLow:{valueAsBoolean}")
    }
  }
  // 创建JSON格式的消息体
  private def createJsonPayload(
                                 value: AlgorithResult,
                                 startTimeOpt: Option[Long],
                                 endTimeOpt: Option[Long]
                               ): String = {

    // 确定告警级别（1=warning，2/3=serious，其他=alarm）
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

    // 使用indexOf和lastIndexOf找到反引号的位置
    // 处理expressions（为空则使用原值）
    val express = if (value.expressions != null && value.expressions.nonEmpty) {
      value.expressions
    }
    // 构建JSON字符串（压缩格式）
    s"""
       |{
       |  "station": "root.ln.`${value.station}`",
       |  "siteId":"${value.station}",
       |  "start_time": ${startTimeOpt.getOrElse("null")},
       |  "end_time": ${endTimeOpt.getOrElse(0L)},
       |  "expressions": "${express}",
       |  "collectedValue": "${value.collectedValue.getOrElse("null")}",
       |  "level": "${level}",
       |  "equip_label":"${value.equip_label}",
       |  "algorithm_id":"${value.algorithm_id}",
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
  private def isBooleanValue(AlgorithResult: AlgorithResult): Boolean = {
    AlgorithResult.collectedValue.exists(_.isInstanceOf[Boolean])
  }

  /**
   * 判断 AlgorithResult 中的 collectedValue 是否包含数值类型
   * 支持的数值类型包括：Int, Long, Float, Double, BigDecimal, Byte, Short
   */
  private def isNumericValue(AlgorithResult: AlgorithResult): Boolean = {
    AlgorithResult.collectedValue.exists { value =>
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
  private def extractBooleanValue(AlgorithResult: AlgorithResult): Option[Boolean] = {
    AlgorithResult.collectedValue match {
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


  // Flink生命周期方法：关闭时调用
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