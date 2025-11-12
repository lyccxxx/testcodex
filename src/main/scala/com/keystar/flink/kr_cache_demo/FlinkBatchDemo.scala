package com.keystar.flink.kr_cache_demo

import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.DataSet

import scala.io.Source
import play.api.libs.json._

import java.time.Instant
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection asJava`
object FlinkBatchDemo {
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
    val result = extractFieldsFromData(json)
    //任务2的字段1
    val result02 = extractFieldsFromData02(json02)
    result.foreach{ case (stationId, fields) =>
      println(s"$stationId -> (${fields.length})")
    }

    println("==========================打印02======================")
    result02.foreach{ case (stationId, fields) =>
      println(s"$stationId -> (${fields.length})")
    }

    println("=================打印合并之后的==========================")
    // 合并两个结果的字段
    val allFields = (result.keySet ++ result02.keySet).foldLeft(Map[String, List[String]]()) {
      (acc, key) =>
        val combinedList = (result.getOrElse(key, List()) ++ result02.getOrElse(key, List())).distinct
        acc + (key -> combinedList)
    }


    allFields.foreach { case (stationId, fields) =>
      println(s"$stationId -> (${fields.length})")
    }

//    val statjs:Map[String, JsArray] = getzcPointList(json)
    //按stationId进行分组获取里边的字段，然后根据字段组成一个dataframe
    val statjs: Map[String, JsArray] = getzcPointList(json)
    // 将 allFields 转换为 DataSet 进行并行处理
    val allFieldsDataSet: DataSet[(String, List[String])] = env.fromCollection(allFields.toList)
//    allFieldsDataSet.map { case (stationId, fields) =>
//      processStationData(stationId, fields, statjs)
////      processGroupedData(fields,statjs)
//    }.collect()
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
            //              val radiation = (zcPoint \ "radiation").asOpt[String].getOrElse("")
                          val power = (zcPoint \ "power").asOpt[String].getOrElse("")
            //              val deviceName = (zcPoint \ "deviceName").asOpt[String].getOrElse("")
            Seq(vField,iField).filter(_.nonEmpty)
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
          var num = 0
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          val allFields = zcPointList.value.flatMap { zcPoint =>
            val vField = (zcPoint \ "nbActPower").asOpt[String].getOrElse("")
            val iField = (zcPoint \ "zcVField").asOpt[String].getOrElse("")
            val zcIField = (zcPoint \ "zcIField").asOpt[String].getOrElse("")
            val power = (zcPoint \ "power").asOpt[String].getOrElse("")
            //              val deviceName = (zcPoint \ "deviceName").asOpt[String].getOrElse("")
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

  // 定义用于处理分组数据的函数
  def processGroupedData(allFields: Map[String, List[String]], statjs: Map[String, JsArray]): Unit = {
    val groupedData = allFields.groupBy(_._1).map { case (stationId, entries) =>
      val fields = entries.flatMap(_._2)
      val lastTsOpt = None // 这里简单假设没有上次时间戳，你可以根据实际情况修改
      var readings: List[IoTDBReading] = List[IoTDBReading]()
      val station = "root.ln.`" + stationId + "`"

      if (fields.size < 300) {
        val query = getSqlQuery(lastTsOpt, fields.mkString(","), station)
        val response = sendRequest(query)
        readings = handleResponse(response)
        val datajs: Option[JsArray] = statjs.get(stationId)
        readings.foreach(iotdb => {
          datajs match {
            case Some(numjs) =>
              val reJson = replaceValues(numjs, iotdb, stationId)
              println(reJson)
            case _ => None
          }
        })
      } else {
        // 大于300的按300一个批次来进行
        val batches = fields.grouped(300).toList
        val jsonData = statjs.getOrElse(stationId, JsArray(Nil))
        val jsonArray = jsonData.value

        for (batch <- batches) {
          // 提取与当前 batch 字段相关的 JSON 数据
          val relevantJsonArray = jsonArray.filter { jsonObj =>
            val jsonFields = extractFieldsFromJson(jsonObj)
            val requiredFields = List("nbActPower", "zcVField", "zcIField")
            val allRequiredFieldsInBatch = requiredFields.forall(field => {
              val fieldValue = (jsonObj.as[JsObject] \ field).asOpt[String]
              fieldValue.exists(batch.contains)
            })
            allRequiredFieldsInBatch
          }

          val batchJson = JsArray(relevantJsonArray)

          val query = getSqlQuery(lastTsOpt, batch.mkString(","), station)
          val response = sendRequest(query)
          val batchReadings = handleResponse(response)
          batchReadings.foreach(iotdb => {
            val reJson = replaceValues(batchJson, iotdb, stationId)
            println(reJson)
          })
          // 获取当前时间
          import java.time.LocalDateTime
          import java.time.format.DateTimeFormatter
          val current = LocalDateTime.now()
          // 定义日期时间格式
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          // 格式化当前时间
          val formattedDateTime = current.format(formatter)
          // 打印格式化后的时间
          println(s"当前时间: $formattedDateTime")
        }
      }
    }
  }

  // 处理单个站点的数据
  def processStationData(stationId: String, fields: List[String], statjs: Map[String, JsArray]): Unit = {
    val lastTsOpt = None // 这里简单假设没有上次时间戳，你可以根据实际情况修改
    var readings: List[IoTDBReading] = List[IoTDBReading]()
    val station = "root.ln.`" + stationId + "`"

    if (fields.size < 300) {
      val query = getSqlQuery(lastTsOpt, fields.mkString(","), station)
      val response = sendRequest(query)
      readings = handleResponse(response)
      val datajs: Option[JsArray] = statjs.get(stationId)
      readings.foreach(iotdb => {
        datajs match {
          case Some(numjs) =>
            val reJson = replaceValues(numjs, iotdb, stationId)
            println(reJson)
          case _ => None
        }
      })
    } else {
      // 大于300的按300一个批次来进行
      val batches = fields.grouped(300).toList
      val jsonData = statjs.getOrElse(stationId, JsArray(Nil))
      val jsonArray = jsonData.value

      for (batch <- batches) {
        // 提取与当前 batch 字段相关的 JSON 数据
        val relevantJsonArray = jsonArray.filter { jsonObj =>
          val jsonFields = extractFieldsFromJson(jsonObj)
          val requiredFields = List("nbActPower", "zcVField", "zcIField")
          val allRequiredFieldsInBatch = requiredFields.forall(field => {
            val fieldValue = (jsonObj.as[JsObject] \ field).asOpt[String]
            fieldValue.exists(batch.contains)
          })
          allRequiredFieldsInBatch
        }

        val batchJson = JsArray(relevantJsonArray)

        val query = getSqlQuery(lastTsOpt, batch.mkString(","), station)
        val response = sendRequest(query)
        val batchReadings = handleResponse(response)
        batchReadings.foreach(iotdb => {
          val reJson = replaceValues(batchJson, iotdb, stationId)
          println(reJson)
        })
        // 获取当前时间
        import java.time.LocalDateTime
        import java.time.format.DateTimeFormatter
        val current = LocalDateTime.now()
        // 定义日期时间格式
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        // 格式化当前时间
        val formattedDateTime = current.format(formatter)
        // 打印格式化后的时间
        println(s"当前时间: $formattedDateTime")
      }
    }
  }


  // 替换值的函数
  def replaceValues(jsonArray: JsArray, reading: IoTDBReading, stationID: String): JsArray = {
    val newJsonArray = jsonArray.value.map { deviceJson =>
      val deviceName = (deviceJson \ "deviceName").as[String]
      val nbActPower = s"root.ln.`${stationID}`." + (deviceJson \ "nbActPower").as[String]
      val zcVField = s"root.ln.`${stationID}`." + (deviceJson \ "zcVField").as[String]
      val zcIField = s"root.ln.`${stationID}`." + (deviceJson \ "zcIField").as[String]
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


  def handleResponse(response: JsValue): List[IoTDBReading] = {
    val expressions = (response \ "expressions").asOpt[List[String]].getOrElse(List.empty)
    val timestamps = (response \ "timestamps").asOpt[List[Long]].getOrElse(List.empty)
    val jsValues = (response \ "values").asOpt[List[JsValue]].getOrElse(List.empty)

    // 解析 values 并确保转换正确
    val parsedValues = jsValues.map(_.as[List[JsValue]].map {
      case JsNumber(n) => Some(n.toDouble)
      case JsString(n) => Some(n.toString)
      case JsNull => null
      case _ => null
    })

    val expressionValueMaps = parsedValues.map(values => expressions.zip(values).toMap)

    // 为每个时间戳创建一个包含所有表达式和值的 IoTDBReading 对象
    val readings = timestamps.map { timestamp =>
      val combinedMap = expressionValueMaps.headOption.getOrElse(Map.empty)
      IoTDBReading(timestamp, combinedMap)
    }
    // 发送读取的数据到 Flink
    readings
  }


  def getzcPointList(json: JsValue): Map[String, JsArray] = {
    (json \ "data").asOpt[JsArray] match {
      case Some(dataArray) =>
        dataArray.value.flatMap { dataObj =>
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          // 对收集到的所有字段进行去重
          val uniqueFields = zcPointList
          List(stationId -> uniqueFields)
        }.toMap
      case None => Map.empty
    }
  }


  def getSqlQuery(lastTsOpt: Option[Option[Long]], iotFlds: String, device: String): String = {
    lastTsOpt match {
      case Some(lastTs) =>
        val nextStartTime = Instant.ofEpochMilli(lastTs.getOrElse(0L)).plusSeconds(15).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs.getOrElse(0L)).plusSeconds(90).toEpochMilli
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case None =>
        s"SELECT ${iotFlds} FROM ${device} ORDER BY time asc LIMIT 5760"
    }
  }
}


/*
* {
* "deviceName":"FZ012-NB07-ZC03"     -- 设备名
*  ,"nbqStatus":null                -- 设备状态
*  ,"nbActPower":"I012_7_INVERTER_ActPower"  -- 设备实际功率
*  ,"zcVField":"I012_7_INVERTER_Line03_U"    -- 线路电压
*  ,"zcIField":"I012_7_INVERTER_Line03_I"    -- 线路电流
*  ,"zcPField":"I012_7_INVERTER_Line03_P"    -- 线路功率  ==》线路功率=线路电压*线路电流
*     ？那这个设备的实际功率是咋算 等于所有线路功率之和？
* }
* */

/**
 * //    val source = new Test04BatchSource()
 * //    source.open(new Configuration())
 * //
 * //    // 从数据源获取数据，并将 Scala List 转换为 Java Collection
 * //    val rulesList: List[DiagnosisRule] = source.getAllRules()
 * //    val rulesJavaCollection: java.util.Collection[DiagnosisRule] = rulesList.asJava  // 转换为 Java Collection
 * //
 * //    // 使用 Java Collection 创建 DataSet
 * //    val rulesDataSet: DataSet[DiagnosisRule] = env.fromCollection(rulesJavaCollection)
 * //    // 关闭数据源
 * //    source.cancel()
 * //    println(s"Number of rules: ${rulesList.size}") // 添加此行来检查获取的规则数量
 * //    // 并行批次请求数据
 * //    val resultDataSet: DataSet[IoTDBReading] = rulesDataSet
 * //      .groupBy(_.iot_tbl)
 * //      .reduceGroup(new DiagnosisRuleReducer()).setParallelism(1)
 * //
 * //    // 打印结果
 * //    resultDataSet.print()
 * */