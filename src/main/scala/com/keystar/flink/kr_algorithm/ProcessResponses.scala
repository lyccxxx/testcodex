package com.keystar.flink.kr_algorithm

import org.eclipse.paho.client.mqttv3.MqttMessage

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object ProcessResponses {
  // 告警状态存储（键：station.expressions，值：(开始时间, 结束时间)）
 // private val alarmStates: TrieMap[String, (Option[Long], Option[Long])] = TrieMap.empty

  // 单线程场景下可用普通HashMap替换
  private val alarmStates: mutable.Map[String, (Option[Long], Option[Long])] = mutable.HashMap.empty

  def main(args: Array[String]): Unit = {
    // 1. 布尔值类型 - 告警开始
    val boolAlarmStart = AlgorithResult(
      timestamp = 1755625425000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(1.0),
      alarm = true,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    // 3. 数字类型 - 告警开始（正数）
    val numberAlarmStart = AlgorithResult(
      timestamp = 1755625440000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(1.0),
      alarm = true,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    val numberAlarmStart01 = AlgorithResult(
      timestamp = 1755625455000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(1.0),
      alarm = true,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    val numberAlarmStart02 = AlgorithResult(
      timestamp = 1755625470000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(1.0),
      alarm = true,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    // 4. 数字类型 - 告警结束（0值）
    val numberAlarmEnd = AlgorithResult(
      timestamp = 1755625620000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(0),
      alarm = false,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    // 5. 数字类型 - 无效值（负数）
    val numberInvalid = AlgorithResult(
      timestamp = 1755626565000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(1.0),
      alarm = true,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    // 6. 字符串类型 - 告警开始（"on"）
    val stringAlarmStart1 = AlgorithResult(
      timestamp = 1755626580000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(1.0),
      alarm = true,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    // 7. 字符串类型 - 告警开始（"1"）
    val stringAlarmStart2 = AlgorithResult(
      timestamp = 1755626610000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(0.0),
      alarm = false,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    // 8. 字符串类型 - 告警结束（"off"）
    val stringAlarmEnd1 = AlgorithResult(
      timestamp = 1755626625000L,
      expressions = "A11_40_isCLXWenKongAbnorm",
      collectedValue = Some(0.0),
      alarm = false,
      equip_label = "A11_40",
      algorithm_id = 20043,
      station = "1521714652626283467"
    )

    // 9. 字符串类型 - 无效值
    val stringInvalid = AlgorithResult(
      timestamp = 1620000960000L,
      expressions = "motor_running",
      collectedValue = Some("unknown"),
      alarm = false,
      equip_label = "motor_controller",
      algorithm_id = 103,
      station = "factory_A_line1"
    )

    // 10. 空表达式情况
    val emptyExpressionAlarm = AlgorithResult(
      timestamp = 1620001080000L,
      expressions = "",
      collectedValue = Some(true),
      alarm = false,
      equip_label = "emergency_button",
      algorithm_id = 105,
      station = "factory_C"
    )

    // 11. 告警重复触发（已结束的告警再次开始）
    val repeatedAlarmStart = AlgorithResult(
      timestamp = 1620001200000L,
      expressions = "temp > 80",
      collectedValue = Some(true),
      alarm = false,
      equip_label = "temperature_sensor",
      algorithm_id = 101,
      station = "factory_A_line1"
    )

    // 12. 告警级别测试（2.0对应serious）
    val seriousLevelAlarm = AlgorithResult(
      timestamp = 1620001320000L,
      expressions = "vibration > 5",
      collectedValue = Some(2.0),
      alarm = false,
      equip_label = "vibration_sensor",
      algorithm_id = 106,
      station = "factory_D"
    )

    // 所有测试数据集合
    val allTestData = List(
      boolAlarmStart, numberAlarmStart01,numberAlarmStart02, numberAlarmEnd,
      numberInvalid, stringAlarmStart1, stringAlarmStart2, stringAlarmEnd1,
      stringInvalid, emptyExpressionAlarm, repeatedAlarmStart, seriousLevelAlarm
    )

    allTestData.foreach(processMessage)

  }

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
    println(s"打印发送的MQTT消息：${outputJson}")
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

}




