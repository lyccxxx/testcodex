package com.keystar.flink.kr_cache_demo

import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import com.keystar.flink.iotdbstream.IoTDBSource.sendRequest
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.DataSet
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import java.util.ArrayList
import scala.io.Source
import play.api.libs.json._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters.asJavaIterableConverter
import java.io.Serializable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class IoTDBReadingData(stationId: String, values: mutable.Map[String, ListBuffer[Option[Any]]])

// 定义一个简单的可序列化类来持有必要的数据
case class SerializableData(stationId: String, fields: List[String], statjsStr: Map[String, String]) extends Serializable
object JsonProcessingJob {
  def main(args: Array[String]): Unit = {
    // 创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "src/main/scala/com/keystar/flink/kr_cache_demo/组串功率入库.json"
    val filePath2 = "src/main/scala/com/keystar/flink/kr_cache_demo/组串最大功率点位.json"
    // 读取文件内容
    val jsonStr = Source.fromFile(filePath).mkString
    val jsonStr02 = Source.fromFile(filePath2).mkString
    // 解析 JSON 字符串
    val json: JsValue = Json.parse(jsonStr)
    val json02: JsValue = Json.parse(jsonStr02)

    // 任务1的字段
    val result: Map[String, List[String]] = extractFieldsFromData(json)
    //任务2的字段1
    val result02: Map[String, List[String]] = extractFieldsFromData02(json02)

    println("打印结果================！===============")
    println(result)
    println(result02)
    println("=======================")

    // 合并两个结果的字段
    val allFields = (result.keySet ++ result02.keySet).foldLeft(Map[String, List[String]]()) {
      (acc, key) =>
        val listFromResult = result.getOrElse(key, List()).distinct // 假设这里已经去重
        val listFromResult02 = result02.getOrElse(key, List())

        // 对 listFromResult02 进行去重，去除那些在 listFromResult 中已存在的元素
        val filteredListFromResult02 = listFromResult02.filterNot(listFromResult.contains)

        // 合并两个列表，保持顺序
        val combinedList = listFromResult ++ filteredListFromResult02

        acc + (key -> combinedList)
    }

//    allFields.foreach { case (stationId, fields) =>
//      println(s"$stationId -> (${fields.length})")
//    }

    // 修改这里，调用修改后的 getzcPointList 方法
    val statjs: Map[String, String] = getzcPointList(json)

    val statjs02: Map[String, String] = getzcPointList(json02)

    // 提取任务1的 I 和 U 字段
//    val task1Data: Map[String, Map[String, (String, String)]] = extractTask1Data(json)

    // 提取任务2的 power 字段
//    val task2Data: Map[String, Map[String, String]] = extractTask2Data(json02)


    val javaCollection = new java.util.ArrayList[(String, List[String])]()
    result.toList.foreach { element =>
      javaCollection.add(element)
    }

    val allFieldsDataSet: DataSet[(String, List[String])] = env.fromCollection(javaCollection).setParallelism(10)


    // 使用显式类型声明来解决编译异常
    val allFieldsDataSet02: DataSet[(String, List[String])] = env.fromCollection(javaCollection)
      .flatMap(new FlatMapFunction[(String, List[String]), (String, List[String])] {
        override def flatMap(value: (String, List[String]), out: Collector[(String, List[String])]): Unit = {
          val (stationId, fields) = value
          if (fields.size > 1000) {
            // 如果字段数量超过1000，则将其分成多个批次
            fields.grouped(1000).foreach(batch => out.collect((stationId, batch.toList)))
          } else {
            // 否则，直接返回原始数据
            out.collect((stationId, fields))
          }
        }
      })
      .setParallelism(10) // 根据实际情况调整并行度



    allFieldsDataSet.map { case (stationId, fields) =>
      processStationData(stationId, fields, statjs,statjs02,result02)
    }.collect()
  }


  // 处理单个站点的数据
  def processStationData(stationId: String, fields: List[String], statjs: Map[String, String],statjs02: Map[String, String],task2Data: Map[String, List[String]]): Unit = {
    val lastTsOpt = None // 这里简单假设没有上次时间戳，你可以根据实际情况修改
    var readings: IoTDBReadingData = IoTDBReadingData(None.getOrElse(""), mutable.Map.empty[String, ListBuffer[Option[Any]]])
    val station = "root.ln.`" + stationId + "`"

    // 将字符串解析为 JsArray
    val jsonDataStr = statjs.getOrElse(stationId, "[]")
    val jsonData = Json.parse(jsonDataStr).as[JsArray]

    val jsonDataStr02 = statjs02.getOrElse(stationId, "[]")
    val jsonData02 = Json.parse(jsonDataStr02).as[JsArray]

    var updatedTask2Data: Map[String, Map[String, List[Double]]]=Map[String, Map[String, List[Double]]]()
    if (fields.size < 300) {
      val task1Data = convertToTask1Data(jsonData, stationId)
      val query = getSqlQuery(lastTsOpt, fields.mkString(","), station,0,1121)
      val response = sendRequest(query)
      readings = handleResponse(response,stationId)
//      updatedTask2Data = calculatePower(task1Data, readings,stationId)
//      val batchjsons=jsonData02.value.grouped(500).toList
//      for (batchjson <- batchjsons){
//        val reJson02 = replaceValues02(batchjson, readings, stationId,updatedTask2Data)
//        //删掉updatedTask2Data里边对应的数值
////        println(reJson02)
//
//      }

      updatedTask2Data=Map.empty

    } else {
      // 大于300的按300一个批次来进行
      val batches = fields.grouped(300).toList

      for (batch <- batches) {
        val jsonArray = jsonData.value
        val jsonArray02: IndexedSeq[JsValue] = jsonData02.value

        // 提取与当前 batch 字段相关的 JSON 数据
        val relevantJsonArray = jsonArray.filter { jsonObj =>
          val requiredFields = List("nbActPower", "zcVField", "zcIField")
          requiredFields.forall(field => (jsonObj \ field).asOpt[String].exists(batch.contains))
        }

        val batchJson: JsArray = JsArray(relevantJsonArray)

        // 获取任务一的数据
        val task1Data = convertToTask1Data(batchJson, stationId)

        val relevantJsonArray02 = jsonArray02.filter { jsonObj =>
          val requiredFields = List("nbqStatus", "nbRunningStatus")
          requiredFields.forall(field => (jsonObj \ field).asOpt[String].exists(batch.contains))
        }

        // 准备并发送SQL查询
        val query = getSqlQuery(lastTsOpt, batch.mkString(","), station,0,5760)
        val response = sendRequest(query)
        val batchReadings = handleResponse(response, stationId)

        // 计算功率数据，并更新到 updatedTask2Data 中
//        val updatedTask2Data02: Map[String, Map[String, List[Double]]] = calculatePower(task1Data, batchReadings, stationId)
//        updatedTask2Data = mergeMaps(updatedTask2Data,updatedTask2Data02)
        // 对第二组JSON数据进行过滤
        if (relevantJsonArray02.nonEmpty) {

          val reJson02 = replaceValues02(relevantJsonArray02, batchReadings, stationId, updatedTask2Data)
          //reJson02调用别的方法
//          println(reJson02)
//          updatedTask2Data = removeProcessedData(reJson02, stationId, updatedTask2Data)
          // 如果相关JSON数据超过300条，则分批处理
//          val rebatchjsons = relevantJsonArray02.grouped(500).toList
//          for (rebatchjson <- rebatchjsons) {
//            // 确保将整个 updatedTask2Data 传入 replaceValues02
//            val reJson02 = replaceValues02(rebatchjson, batchReadings, stationId, updatedTask2Data)
//            //reJson02调用别的方法
//            println(reJson02)
//            updatedTask2Data = removeProcessedData(reJson02, stationId, updatedTask2Data)
//          }
        }
      }
    }
      //可以先获取，然后里边需要的json,就删掉，存储新的

      updatedTask2Data=Map.empty
      // 获取当前时间
      val current = LocalDateTime.now()
      // 定义日期时间格式
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      // 格式化当前时间
      val formattedDateTime = current.format(formatter)
      // 打印格式化后的时间
      println(s"当前时间: $formattedDateTime")
  }

  // 新增方法：用于从 updatedTask2Data 中移除已处理的数据
  def removeProcessedData(jsonArray: JsArray, stationId: String, updatedTask2Data: Map[String, Map[String, List[Double]]]): Map[String, Map[String, List[Double]]] = {
    jsonArray.value.foldLeft(updatedTask2Data) { (acc, jsonObj) =>
      val deviceName = (jsonObj \ "deviceName").as[String]

      // 获取站点对应的数据
      acc.get(stationId).map { deviceMap =>
        if (deviceMap.contains(deviceName)) {
          // 如果该设备的数据存在，则移除它
          val newDeviceMap = deviceMap - deviceName
          if (newDeviceMap.isEmpty) {
            // 如果移除后没有其他设备的数据，则移除整个站点的数据
            acc - stationId
          } else {
            // 否则，更新站点的数据
            acc + (stationId -> newDeviceMap)
          }
        } else {
          // 如果该设备的数据不存在，则保持原样
          acc
        }
      }.getOrElse(acc) // 如果站点ID不存在于acc中，则保持原样
    }
  }

  def mergeMaps(map1: Map[String, Map[String, List[Double]]], map2: Map[String, Map[String, List[Double]]]): Map[String, Map[String, List[Double]]] = {
    (map1.keySet ++ map2.keySet).foldLeft(Map[String, Map[String, List[Double]]]()) { (acc, key) =>
      val combinedInnerMap = (map1.getOrElse(key, Map()), map2.getOrElse(key, Map())) match {
        case (innerMap1, innerMap2) =>
          // 合并内层 Map
          (innerMap1.keySet ++ innerMap2.keySet).foldLeft(Map[String, List[Double]]()) { (innerAcc, innerKey) =>
            val list1 = innerMap1.getOrElse(innerKey, List())
            val list2 = innerMap2.getOrElse(innerKey, List())
            // 合并两个列表，去除重复项（假设你需要这样做）
            val combinedList = (list1 ++ list2)
            innerAcc + (innerKey -> combinedList)
          }
      }
      acc + (key -> combinedInnerMap)
    }
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



  // 提取 data 里面的字段
  def extractFieldsFromData02(json: JsValue): Map[String, List[String]] = {
    (json \ "data").asOpt[JsArray] match {
      case Some(dataArray) =>
        dataArray.value.flatMap { dataObj =>
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          val allFields = zcPointList.value.flatMap { zcPoint =>
            val vField = (zcPoint \ "nbqStatus").asOpt[String].getOrElse("")
            val iField = (zcPoint \ "nbRunningStatus").asOpt[String].getOrElse("")
            Seq(vField, iField).filter(_.nonEmpty)
          }.toList
          // 对收集到的所有字段进行去重
          val uniqueFields = allFields.distinct.toList
          List(stationId -> uniqueFields)
        }.toMap
      case None => Map.empty
    }
  }

  //获取iotdb的字段
  def extractFieldsFromData(json: JsValue): Map[String, List[String]] = {
    (json \ "data").asOpt[JsArray] match {
      case Some(dataArray) =>
        dataArray.value.flatMap { dataObj =>
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          val allFields = zcPointList.value.flatMap { zcPoint =>
            val vField = (zcPoint \ "nbActPower").asOpt[String].getOrElse("")
            val iField = (zcPoint \ "zcVField").asOpt[String].getOrElse("")
            val zcIField = (zcPoint \ "zcIField").asOpt[String].getOrElse("")
            val power = (zcPoint \ "power").asOpt[String].getOrElse("")
            Seq(vField, iField, zcIField).filter(_.nonEmpty)
          }.toList
          // 对收集到的所有字段进行去重
          val uniqueFields = allFields.distinct.toList
          List(stationId -> uniqueFields)
        }.toMap
      case None => Map.empty
    }
  }

  def extractFieldsFromJson(jsonObj: JsValue): List[String] = {
    jsonObj.as[JsObject].fields.flatMap {
      case (_, value) =>
        value match {
          case JsString(str) => Some(str)
          case _ => None
        }
    }.toList
  }

  def calculatePower(
                      task1Data: Map[String, Map[String, (String, String)]],
                      iotdbReading: IoTDBReadingData,
                      stationId: String
                    ): Map[String, Map[String, List[Any]]] = {

    // 首先从 task1Data 中获取指定站点 ID 的设备映射
    task1Data.get(stationId).map { deviceMap =>
      // 获取时间戳列表作为基准长度
      val timestamps: Seq[Long] = iotdbReading.values.getOrElse("timestamp", List.empty[Option[Any]])
        .flatten // 去除 Option 包装
        .flatMap {
          case str: String =>
            try {
              Some(str.toLong)
            } catch {
              case _: NumberFormatException => None
            }
          case num: Number => Some(num.longValue())
          case _ => None
        }
      // 遍历设备映射，计算每个设备的功率
      val updatedDeviceMap = deviceMap.map { case (deviceName, (iField, uField)) =>
        if (iField.nonEmpty && uField.nonEmpty) {
          // 从 IoTDBReading 中安全地获取 I 和 U 的值列表
          val iValues = iotdbReading.values.getOrElse(s"root.ln.`$stationId`.$iField", List.fill(timestamps.length)(None)).collect { case Some(v) => v.toString.toDouble }
          val uValues = iotdbReading.values.getOrElse(s"root.ln.`$stationId`.$uField", List.fill(timestamps.length)(None)).collect { case Some(v) => v.toString.toDouble }

          // 如果 I 和 U 的长度不一致或者为空，填充默认值 0.0
          val filledIVals = if (iValues.length < timestamps.length) iValues.padTo(timestamps.length, 0.0) else iValues
          val filledUVals = if (uValues.length < timestamps.length) uValues.padTo(timestamps.length, 0.0) else uValues

          // 计算功率值
          val powerValues = filledIVals.zip(filledUVals).map { case (i, u) => u * i }

          // 返回设备名称和计算得到的功率列表的映射
          deviceName -> powerValues.toList
        } else {
          // 如果 I 或 U 字段为空，返回默认值 0.0 的列表
          deviceName -> List.fill(timestamps.length)(0.0)
        }
      }.filterNot(_._2.isEmpty) // 过滤掉功率列表为空的结果

      val newupdateMap= updatedDeviceMap++Map("timestamp"->timestamps.toList)
      // 返回更新后的站点 ID 和设备功率映射
      Map(stationId -> newupdateMap)
    }.getOrElse(Map.empty)
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



  // 替换值的函数
  def replaceValues(jsonArray: JsArray, reading: IoTDBReading, stationID: String): JsArray = {
    val newJsonArray = jsonArray.value.map { deviceJson =>
      val deviceName = (deviceJson \ "deviceName").as[String]
      val nbActPower = s"root.ln.`${stationID}`." + (deviceJson \ "nbActPower").as[String]
      val zcVField = s"root.ln.`${stationID}`." + (deviceJson \ "zcVField").as[String]
      val zcIField = s"root.ln.`${stationID}`." + (deviceJson \ "zcIField").as[String]
      reading.values
      val newDeviceJson = reading.values.foldLeft(deviceJson) { case (json, (key, value)) =>
        if (key.contains(nbActPower)) {
          val fieldName = key.split("\\.").last
          value match {
            case Some(v) => json.as[JsObject] + ("nbActPower" -> JsString(v.toString))
            case None => json
          }
        } else if (key.contains(zcVField)) {
          val fieldName = key.split("\\.").last
          value match {
            case Some(v) => json.as[JsObject] + ("zcVField" -> JsString(v.toString))
            case None => json
          }
        } else if (key.contains(zcIField)) {
          val fieldName = key.split("\\.").last
          value match {
            case Some(v) => json.as[JsObject] + ("zcIField" -> JsString(v.toString))
            case None => json
          }
        } else {
          json
        }
      }
      // 添加 timestamp 字段
      val updatedObj = newDeviceJson.as[JsObject] + ("timestamp" -> JsNumber(reading.timestamp))
      updatedObj
    }
    JsArray(newJsonArray)
  }

  def replaceValues02(jsonArray: IndexedSeq[JsValue], reading: IoTDBReadingData, stationID: String, power: Map[String, Map[String, List[Any]]]): JsArray = {

    val newJsonArray = jsonArray.map { deviceJson =>
      val deviceName = (deviceJson \ "deviceName").as[String]
      val nbqStatus = "root.ln.`"+stationID+"`." + (deviceJson \ "nbqStatus").as[String]
      val nbRunningStatus = "root.ln.`"+stationID+"`." + (deviceJson \ "nbRunningStatus").as[String]

      // 从 power 中获取该设备的功率值
      val devicePower: List[Double] = power.getOrElse(stationID, Map())
        .getOrElse(deviceName, List())
        .map {
          case x: Double => x
          case _ => 0.0 // 如果不是 Double 类型，则使用默认值 0.0
        }

      var newDeviceJson = deviceJson
      // 更新 nbqStatus 字段
      reading.values.find { case (key, _) => key.contains(nbqStatus) }.foreach { case (_, value) =>
        val nonNullValues = value.collect { case Some(v) => JsString(v.toString) }
        if(nonNullValues.nonEmpty) {
          newDeviceJson = newDeviceJson.as[JsObject] + ("nbqStatus" -> JsArray(nonNullValues))
        }
      }
      // 更新 nbRunningStatus 字段
      reading.values.find { case (key, _) => key.contains(nbRunningStatus) }.foreach { case (_, value) =>
        val nonNullValues = value.collect { case Some(v) => JsString(v.toString) }
        if(nonNullValues.nonEmpty) {
          newDeviceJson = newDeviceJson.as[JsObject] + ("nbRunningStatus" -> JsArray(nonNullValues))
        }
      }

      reading.values.find { case (key, _) => key.contains("timestamp") }.foreach { case (_, value) =>
        val nonNullValues = value.collect { case Some(v) => JsString(v.toString) }
        if(nonNullValues.nonEmpty) {
          newDeviceJson = newDeviceJson.as[JsObject] + ("timestamp" -> JsArray(nonNullValues))
        }
      }

      // 更新 power 字段
      val powerJsArray = JsArray(devicePower.map(JsNumber(_)))
      newDeviceJson = newDeviceJson.as[JsObject] + ("power" -> powerJsArray)

      // 添加 timestamp 字段
      val updatedObj02 = newDeviceJson.as[JsObject] + ("stationId" -> JsString(stationID))
      updatedObj02
    }
    JsArray(newJsonArray)
  }

  def handleResponse(response: JsValue, stationId: String): IoTDBReadingData = {
    val expressions = (response \ "expressions").asOpt[List[String]].getOrElse(List.empty)
    val timestamps = (response \ "timestamps").asOpt[List[Long]].getOrElse(List.empty)
    val jsValues = (response \ "values").asOpt[List[List[JsValue]]].getOrElse(List.empty)

    // 遍历每个子列表
    val jsvnum2=jsValues.map { subList =>
      // 遍历子列表中的每个元素
      subList.map { jsValue =>
        jsValue match {
          // 如果是 JsNumber 类型，检查其值是否为负数
          case JsNumber(n) if n.toDouble < 0 => JsNumber(0.0)
          // 如果是 JsNull 类型，将其替换为 JsNumber(0)
          case JsNull => JsNumber(0.0)
          // 其他情况保持原值
          case _ => jsValue
        }
      }
    }
    // 检查表达式数量是否与值的数量一致
    if (expressions.length != jsValues.length) {
      throw new IllegalArgumentException("The number of expressions does not match the number of value lists.")
    }

    // 使用可变 Map 和 List 来减少数据复制
    import scala.collection.mutable
    val combinedValuesMap: mutable.Map[String, mutable.ListBuffer[Option[Any]]] = mutable.Map.empty
    expressions.foreach(expr => combinedValuesMap(expr) = mutable.ListBuffer.empty)
    combinedValuesMap("timestamp") = mutable.ListBuffer.empty

    // 解析每个表达式的值，并确保转换正确
    def parseValue(jsValue: JsValue): Option[Any] = jsValue match {
      case JsNumber(n) =>
        val value = n.toDouble
        if (value < 0) Some(0.0) else Some(value)
      case JsString(s) => Some(s)
      case JsBoolean(b) => Some(b)
      case JsNull => None
      case _ => None
    }

    // 确保 timestamps 和每个表达式的值列表长度一致
    if (timestamps.length != jsValues.headOption.map(_.length).getOrElse(0)) {
      throw new IllegalArgumentException("Timestamps and values lists do not match in length.")
    }

    val current = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    // 格式化当前时间
    val formattedDateTime = current.format(formatter)
    // 打印格式化后的时间
    println(s"当前查询iotdb转换map之前时间: $formattedDateTime")
    // 并行处理数据
    jsValues.zip(expressions).par.foreach { case (values, expr) =>
      values.foreach { jsValue =>
        combinedValuesMap(expr) += parseValue(jsValue)
      }
    }

    timestamps.foreach { timestamp =>
      combinedValuesMap("timestamp") += Some(timestamp)
    }

    val current02 = LocalDateTime.now()

    // 打印格式化后的时间
    println(s"当前查询iotdb转换map之后时间: ${current02.format(formatter)}")
    // 返回重组后的数据
    IoTDBReadingData(stationId, combinedValuesMap)
  }
  // 修改这个方法，返回 Map[String, String]
  def getzcPointList(json: JsValue): Map[String, String] = {
    (json \ "data").asOpt[JsArray] match {
      case Some(dataArray) =>
        dataArray.value.flatMap { dataObj =>
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          // 将 JsArray 转换为 String
          val zcPointListStr = Json.stringify(zcPointList)
          List(stationId -> zcPointListStr)
        }.toMap

      case None => Map.empty
    }
  }

  def getSqlQuery(lastTsOpt: Option[Long], iotFlds: String, device: String,offset: Int, limit: Int): String = {
    lastTsOpt match {
      case Some(lastTs) =>
        val nextStartTime = java.time.Instant.ofEpochMilli(lastTs).plusSeconds(15).toEpochMilli
        val nextEndTime = java.time.Instant.ofEpochMilli(lastTs).plusSeconds(90).toEpochMilli
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case None =>
        s"SELECT ${iotFlds} FROM ${device} ORDER BY time  LIMIT ${limit} OFFSET ${offset};"
        //-- where time < 1723178745000 and  time>=1723092345000 5760
    }
  }
}