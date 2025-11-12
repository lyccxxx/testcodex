package com.keystar.flink.kr_cache_demo

import com.keystar.flink.kr_cache_demo.JsonProcessingJob.{extractFieldsFromData, extractFieldsFromData02}
import play.api.libs.json._

import scala.io.Source
import org.apache.flink.api.scala._

case class IoTDBReading(timestamp: Long, values: Map[String, Option[Any]])

object CacheSource {
  def main(args: Array[String]): Unit = {
    // 创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 读取任务1和任务2的JSON文件
    val filePath1 = "src/main/scala/com/keystar/flink/kr_cache_demo/组串功率入库.json"
    val filePath2 = "src/main/scala/com/keystar/flink/kr_cache_demo/组串最大功率点位.json"
    val jsonStr1 = Source.fromFile(filePath1).mkString
    val jsonStr2 = Source.fromFile(filePath2).mkString

    // 解析JSON
    val json1: JsValue = Json.parse(jsonStr1)
    val json2: JsValue = Json.parse(jsonStr2)

    // 提取任务1的 I 和 U 字段
    // 任务1的字段
    val result = extractFieldsFromData(json1)
    //任务2的字段1
    val result02 = extractFieldsFromData02(json2)
    result.foreach { case (stationId, fields) =>
      println(s"$stationId -> (${fields.length})")
    }
    println("==========zg1========================")
    result02.foreach { case (stationId, fields) =>
      println(s"$stationId -> (${fields.length})")
    }
val data = Map(
  "1816341324371066880" -> Map(
    "timestamp" -> List(1725064740000L, 7250647400000L),
    "B06_YC0035_datasample01" -> List(12.1, 13.456),
    "B06_YC0036_datasample01" -> List(14.4, 15.345),
    "B06_YC0037_datasample01" -> List(16.8, 17.123)
  )
)
    val task2Data = Map(
      "1816341324371066880" -> Map(
        "B06_YC0035_datasample01" -> "B06_YC0035_datasample",
        "B06_YC0036_datasample01" -> "B06_YC0036_datasample",
        "B06_YC0037_datasample01" -> "B06_YC0037_datasample",
        "B06_YC0038_datasample01" -> "B06_YC0038_datasample",
        "B06_YC0039_datasample01" -> "B06_YC0039_datasample"
      )
    )

    println(convertToIoTDBJson(data, "1816341324371066880", task2Data))

    val map1 = Map(
      "1821383836034924544" -> Map(
        "FZ003-NB01-ZC0918" -> ("I001_1_YX0041", "I001_1_YX0003"),
        "FZ003-NB01-ZC0703" -> ("I001_1_YX0040", "I003_1_YX0037")
      )
    )

    // 定义 map2
    val map2 = Map(
      "1821383836034924544" -> Map(
        "timestamp" -> List(1725064740000L, 7250647400000L),
        "FZ003-NB01-ZC0918" -> List(12.1, 13.456),
        "FZ003-NB01-ZC0703" -> List(14.4, 15.345)
      )
    )

    val stationID = "1821383836034924544"
    // 调用处理函数
    // 从 map2 中获取 stationID 对应的键（排除 "timestamp"）
    val keysFromMap2 = map2(stationID).keys.filter(_ != "timestamp").toList

    // 从 map1 中根据 keysFromMap2 获取对应的字段值
    val fieldMappings = map1(stationID)
    val result05 = keysFromMap2
      .flatMap(key => fieldMappings.get(key).toList) // 获取对应键的字段值
      .flatMap { case (a, b) => List(a, b) } // 展开字段对
      .distinct // 去重
    println(result05)

    val strTimestamp = "1234567890"
    try {
      val longTimestamp = strTimestamp.toLong
      // 处理转换后的 Long 类型时间戳
      println("能正常转换")
    } catch {
      case e: NumberFormatException =>
        // 处理转换失败的情况
        println(s"无法将字符串 $strTimestamp 转换为 Long 类型: ${e.getMessage}")
    }
  }


  import play.api.libs.json._



  def convertToIoTDBJson(data: Map[String, Map[String, List[Any]]], stationID: String, task2Data: Map[String, Map[String, String]]): String = {
    // 提取所有的时间戳
    val timestamps: List[Long] = data.get(stationID).flatMap(_.get("timestamp").map(_.asInstanceOf[List[Long]])).getOrElse(List.empty)

    // 获取任务映射
    val taskMap: Option[Map[String, String]] = task2Data.get(stationID)

    // 获取 taskMap 中所有的目标测量名称
    val existingMeasurementNames = taskMap.map(_.values.toSet).getOrElse(Set.empty)

    // 过滤掉时间戳和 taskMap 中已有的测量名称
    val validMeasurements = data.get(stationID).map { stationData =>
      stationData.filterKeys(key => key != "timestamp" && !existingMeasurementNames.contains(key)).keys.toList
    }.getOrElse(List.empty)

    // 构建 measurements_list, data_types_list, 和 values_list
    val (measurementsList, dataTypesList, valuesList) = {
      val measurementNames = validMeasurements
      val dataTypeNames = List.fill(measurementNames.size)("FLOAT") // 假设数据类型为 FLOAT

      val valueListsPerTimestamp = timestamps.zipWithIndex.map { case (_, index) =>
        measurementNames.map { measurementName =>
          data.get(stationID).flatMap(_.get(measurementName)) match {
            case Some(values) if values.isDefinedAt(index) =>
              values(index) match {
                case num: Double => num
                case num: Float => num.toDouble
                case _ => null.asInstanceOf[Double]
              }
            case _ => null.asInstanceOf[Double]
          }
        }
      }

      (List.fill(timestamps.size)(measurementNames),
        List.fill(timestamps.size)(dataTypeNames),
        valueListsPerTimestamp)
    }

    // 设备名称
    val devices: List[String] = List.fill(timestamps.size)(s"root.ln.`$stationID`")

    // 构建最终的 JSON 对象
    val json = Json.obj(
      "timestamps" -> JsArray(timestamps.map(JsNumber(_))),
      "measurements_list" -> JsArray(measurementsList.map(list => JsArray(list.map(JsString)))),
      "data_types_list" -> JsArray(dataTypesList.map(list => JsArray(list.map(JsString)))),
      "values_list" -> JsArray(valuesList.map(list => JsArray(list.map(JsNumber(_))))),
      "is_aligned" -> false,
      "devices" -> JsArray(devices.map(JsString))
    )

    // 将 JSON 对象转换为字符串
    Json.stringify(json)
  }
  // 合并两个嵌套的 Map
  def mergeMaps(map1: Map[String, Map[String, List[Double]]], map2: Map[String, Map[String, List[Double]]]): Map[String, Map[String, List[Double]]] = {
    (map1.keySet ++ map2.keySet).foldLeft(Map[String, Map[String, List[Double]]]()) { (acc, key) =>
      val combinedInnerMap = (map1.getOrElse(key, Map()), map2.getOrElse(key, Map())) match {
        case (innerMap1, innerMap2) =>
          // 合并内层 Map
          (innerMap1.keySet ++ innerMap2.keySet).foldLeft(Map[String, List[Double]]()) { (innerAcc, innerKey) =>
            val list1 = innerMap1.getOrElse(innerKey, List())
            val list2 = innerMap2.getOrElse(innerKey, List())
            // 合并两个列表，去除重复项（假设你需要这样做）
            val combinedList = (list1 ++ list2).distinct
            innerAcc + (innerKey -> combinedList)
          }
      }
      acc + (key -> combinedInnerMap)
    }
  }

  def handleResponse(response: JsValue): List[IoTDBReading] = {
    val expressions = (response \ "expressions").asOpt[List[String]].getOrElse(List.empty)
    val timestamps = (response \ "timestamps").asOpt[List[Long]].getOrElse(List.empty)
    val jsValues = (response \ "values").asOpt[List[List[JsValue]]].getOrElse(List.empty)

    // 检查表达式数量是否与值的数量一致
    if (expressions.length != jsValues.length) {
      throw new IllegalArgumentException("The number of expressions does not match the number of value lists.")
    }

    // 解析每个表达式的值，并确保转换正确
    val parsedValuesLists = jsValues.map(_.map {
      case JsNumber(n) => Some(n.toDouble)
      case JsString(s) => Some(s.toString)
      case JsBoolean(b)=> Some(b.toString)
      case JsNull => None
      case _ => None
    })

    // 确保 timestamps 和每个表达式的值列表长度一致
    if (timestamps.length != parsedValuesLists.headOption.map(_.length).getOrElse(0)) {
      throw new IllegalArgumentException("Timestamps and values lists do not match in length.")
    }

    // 重组数据以生成 IoTDBReading 对象
    val readings = for {
      (timestamp, timeIndex) <- timestamps.zipWithIndex
    } yield {
      // 获取当前时间戳下所有表达式的值
      val combinedMap: Map[String, Option[Any]] = expressions.zipWithIndex.map { case (expr, exprIndex) =>
        expr -> parsedValuesLists(exprIndex)(timeIndex)
      }.toMap
      IoTDBReading(timestamp, combinedMap)
    }

    readings
  }

  def replaceValues02(jsonArray: JsArray, reading: IoTDBReading, stationID: String, power: Map[String, Map[String, Double]]): JsArray = {
    val newJsonArray = jsonArray.value.map { deviceJson =>
      val deviceName = (deviceJson \ "deviceName").as[String]
      val nbqStatus = "root.ln.`"+stationID+"`." + (deviceJson \ "nbqStatus").as[String]
      val nbRunningStatus = "root.ln.`"+stationID+"`." + (deviceJson \ "nbRunningStatus").as[String]
      // 从 power 中获取该设备的功率值
      val devicePower = power.get(stationID).flatMap(_.get(deviceName)).getOrElse(0.0)

      var newDeviceJson = deviceJson
      // 更新 nbqStatus 字段
      reading.values.find { case (key, _) => key.contains(nbqStatus) }.foreach { case (_, value) =>
        value.foreach { v =>
          newDeviceJson = newDeviceJson.as[JsObject] + ("nbqStatus" -> JsString(v.toString))
        }
      }
      // 更新 nbRunningStatus 字段
      reading.values.find { case (key, _) => key.contains(nbRunningStatus) }.foreach { case (_, value) =>
        value.foreach { v =>
          newDeviceJson = newDeviceJson.as[JsObject] + ("nbRunningStatus" -> JsString(v.toString))
        }
      }
      // 更新 power 字段
      newDeviceJson = newDeviceJson.as[JsObject] + ("power" -> JsNumber(devicePower))

      // 添加 timestamp 字段
      val updatedObj = newDeviceJson.as[JsObject] + ("timestamp" -> JsNumber(reading.timestamp))
      updatedObj
    }
    JsArray(newJsonArray)
  }

  def convertToTask1Data(jsonArray: JsArray, stationId: String): Map[String, Map[String, (String, String)]] = {
    // 将JsArray中的每个JsValue转换为所需的Map结构
    val task1Data: Map[String, Map[String, (String, String)]] = jsonArray.value.map { json =>
      val deviceName = (json \ "deviceName").as[String]
      val zcVField = (json \ "zcVField").as[String]
      val zcIField = (json \ "zcIField").as[String]

      // 构建内层Map
      val deviceInfo = Map(deviceName -> (zcVField, zcIField))

      // 返回一个Pair，用于构建最终的Map
      stationId -> deviceInfo
    }.foldLeft(Map.empty[String, Map[String, (String, String)]]) {
      case (acc, (id, deviceMap)) =>
        // 合并具有相同stationId的设备信息
        val updatedDevices = acc.getOrElse(id, Map.empty) ++ deviceMap
        acc + (id -> updatedDevices)
    }
    task1Data
  }

  // 提取任务1的 I 和 U 字段
  def extractTask1Data(json: JsValue): Map[String, Map[String, (String, String)]] = {
    (json \ "data").asOpt[JsArray] match {
      case Some(dataArray) =>
        dataArray.value.flatMap { dataObj =>
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          val deviceMap = zcPointList.value.map { zcPoint =>
            val deviceName = (zcPoint \ "deviceName").as[String]
            val zcIField = (zcPoint \ "zcIField").as[String] // 电流字段
            val zcVField = (zcPoint \ "zcVField").as[String] // 电压字段
            deviceName -> (zcIField, zcVField)
          }.toMap
          List(stationId -> deviceMap)
        }.toMap
      case None => Map.empty
    }
  }

  // 提取任务2的 power 字段
  def extractTask2Data(json: JsValue): Map[String, Map[String, String]] = {
    (json \ "data").asOpt[JsArray] match {
      case Some(dataArray) =>
        dataArray.value.flatMap { dataObj =>
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          val deviceMap = zcPointList.value.map { zcPoint =>
            val deviceName = (zcPoint \ "deviceName").as[String]
            val power = (zcPoint \ "power").as[String] // 功率字段
            deviceName -> power
          }.toMap
          List(stationId -> deviceMap)
        }.toMap
      case None => Map.empty
    }
  }

  // 计算 P 并更新任务2的数据
  def calculatePower(
                      task1Data: Map[String, Map[String, (String, String)]],
                      task2Data: Map[String, Map[String, String]],
                      iotdbReading: IoTDBReading
                    ): Map[String, Map[String, Double]] = {
    task2Data.map { case (stationId, deviceMap) =>
      val updatedDeviceMap = deviceMap.map { case (deviceName, powerField) =>
        // 获取任务1中的 I 和 U 字段
        val (iField, uField) = task1Data.get(stationId)
          .flatMap(_.get(deviceName))
          .getOrElse(("", ""))

        // 从 IoTDBReading 中获取 I 和 U 的值
        val iValue: Double = iotdbReading.values.get(s"root.ln.`$stationId`.$iField") match {
          case Some(Some(doubleValue)) => doubleValue.toString.toDouble
          case _ => 0.0
        }
        val uValue = iotdbReading.values.get(s"root.ln.`$stationId`.$uField")match {
          case Some(Some(doubleValue)) => doubleValue.toString.toDouble
          case _ => 0.0
        }
        if(uValue!=0.0){
          val power02=uValue * iValue
          println(power02)
        }
        // 计算功率 P = U * I
        val power = uValue * iValue

        // 更新 power 字段
        deviceName -> power
      }
      stationId -> updatedDeviceMap
    }
  }
}
