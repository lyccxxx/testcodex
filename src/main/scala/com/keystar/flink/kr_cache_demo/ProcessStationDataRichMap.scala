package com.keystar.flink.kr_cache_demo

import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.iotdbstream.IoTDBSource.sendRequest
import com.keystar.flink.kr_cache_demo.JsonProcessingJob.{calculatePower, convertToTask1Data, extractFieldsFromData02, extractTask2Data, getSqlQuery, handleResponse, processStationData, replaceValues02}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.rocksdb.{Options, RocksDB}
import play.api.libs.json.{JsArray, JsNumber, JsString, JsValue, Json}

import java.net.{HttpURLConnection, URL}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.:+
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ProcessStationDataRichMap (statjs: Map[String, String], statjs02: Map[String, String],task3Data: Map[String, Map[String, String]],tjson2data: Map[String, Map[String, (String, String)]])
  extends RichMapFunction[(String, List[String]), Unit] {
  private var rocksDB: RocksDB = _

  override def open(parameters: Configuration): Unit = {
    val dbOptions = new Options().setCreateIfMissing(true)
//    rocksDB = RocksDB.open(dbOptions, "F:\\rocksdb")
//    task2DataMap=extractTask2Data(statjs02)

  }

  val task2Data: Map[String, List[String]]=Map()
  override def map(value: (String, List[String])): Unit = {
    //上游输入 站点 和字段列表
    val (stationId, fields) = value
    processStationData(stationId, fields, statjs, statjs02,tjson2data)
  }

  def processStationData(stationId: String, fields: List[String], statjs: Map[String, String], statjs02: Map[String, String],tjsondata: Map[String, Map[String, (String, String)]]): Unit = {
    val lastTsOpt = None // 这里简单假设没有上次时间戳，你可以根据实际情况修改
    var readings: IoTDBReadingData = IoTDBReadingData(None.getOrElse(""), mutable.Map.empty[String, ListBuffer[Option[Any]]])
    var readings2: IoTDBReadingData = IoTDBReadingData(None.getOrElse(""), mutable.Map.empty[String, ListBuffer[Option[Any]]])

    val station = "root.ln.`" + stationId + "`"

    val jsonDataStr = statjs.getOrElse(stationId, "[]")
    val jsonData = Json.parse(jsonDataStr).as[JsArray]

    val jsonDataStr02 = statjs02.getOrElse(stationId, "[]")
    val jsonData02 = Json.parse(jsonDataStr02).as[JsArray]

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    //获取json2里边各个站点的状态值
    val statMap1: Map[String, Map[String, (String, String)]] = tjson2data

    //创建接收power的集合
    var updatedTask1Data: Map[String, Map[String, List[Any]]] = Map()
    if (updatedTask1Data == null) {
      updatedTask1Data = Map[String, Map[String, List[Any]]]()
    }
    val queryRanges = List(
      (0, 5760)
    )


    if (fields.size < 300) {
      //获取到任务1需要的map
      val task1Data: Map[String, Map[String, (String, String)]] = convertToTask1Data(jsonData, stationId)
      for ((offset, limit) <- queryRanges) {
        val query = getSqlQuery(lastTsOpt, fields.mkString(","), station,offset, limit)
        val current = LocalDateTime.now()
        // 格式化当前时间
        val formattedDateTime = current.format(formatter)
        // 打印格式化后的时间
        println(s"当前查询iotdb之前时间: $formattedDateTime")
        val response = sendRequest(query)

        val batchReadings: IoTDBReadingData = handleResponse(response, stationId)

        val current02 = LocalDateTime.now()
        val formattedDateTime02 = current02.format(formatter)
        // 打印格式化后的时间
        println(s"当前查询iotdb之后的时间: $formattedDateTime02")
        // 计算功率数据，并更新到 updatedTask2Data 中  //更新计算也需要时间2秒
        val updatedTask2Data: Map[String, Map[String, List[Any]]] = calculatePower(task1Data, batchReadings, stationId)
//        if(updatedTask1Data.isEmpty){
//          updatedTask1Data=updatedTask2Data
//        }else{
//          updatedTask1Data = mergeMaps(updatedTask1Data, updatedTask2Data)
//        }
        //将功率的数据写入到数据库里边
        //插入iotdb
        val current04 = LocalDateTime.now()
        println(s"当前查询iotdb插入之前的时间: ${current04.format(formatter)}")
        convertToIoTDBJson(updatedTask2Data, stationID = stationId, task2Data = task3Data)
        val current03 = LocalDateTime.now()
        println(s"当前查询iotdb插入之后的时间: ${current03.format(formatter)}")
      }


//      // 从 map2 中获取 stationID 对应的键（排除 "timestamp"）
//      val keysFromMap2 = updatedTask1Data(stationId).keys.filter(_ != "timestamp").toList
//
//      // 从 map1 中根据 keysFromMap2 获取对应的字段值
//      val fieldMappings = statMap1(stationId)
//      val json2List = keysFromMap2
//        .flatMap(key => fieldMappings.get(key).toList) // 获取对应键的字段值
//        .flatMap { case (a, b) => List(a, b) } // 展开字段对
//        .distinct // 去重
//
//      readings2=IoTDBReadingData(None.getOrElse(""), Map.empty[String, List[Option[Any]]])
//      // 检查 json2List 中的字段是否存在于 IoTDBReadingData 的 values 中
//      val missingFields = json2List.filterNot(readings2.values.contains)
//
//      // 输出需要反查数据库的字段
//      println(s"需要反查数据库的字段: $missingFields")
//
//      // 批量查询数据库（假设有一个函数 batchQueryFromDB 可以批量查询）
//      if (missingFields.nonEmpty) {
//        val query = getSqlQuery(lastTsOpt, missingFields.mkString(","), station,0,1121)
//        val response = sendRequest(query)
//        val read_new: IoTDBReadingData = handleResponse(response, stationId)
//
//        // 合并原有数据和新的查询结果
//        // 如果 readings2 为空，则直接使用 read_new；否则进行合并
////        val updatedReadings = if (readings2.values.isEmpty) {
////          read_new
////        } else {
////          // 合并原有数据和新的查询结果
////          val updatedValues = readings2.values ++ read_new.values
////          readings2.copy(values = updatedValues)
////        }
//        val updatedReadings=read_new
//
//
//        val site_jsonData: IndexedSeq[JsValue] = jsonData02.value.filter { jsonObj =>
//          val deviceName = (jsonObj \ "deviceName").asOpt[String]
//          deviceName.exists(keysFromMap2.contains)
//        }
//
//        if(site_jsonData.size>1000){
//          //开始组合json往Python那边发送
//          val rebatchjsons = site_jsonData.grouped(500).toList
//          for (rebatchjson <- rebatchjsons) {
//            // 确保将整个 updatedTask2Data 传入 replaceValues02
//            val reJson02 = replaceValues02(rebatchjson, updatedReadings, stationId, updatedTask1Data)
//            //reJson02调用别的方法
////            println(reJson02)
//          }
//        }else{
//          // 确保将整个 updatedTask2Data 传入 replaceValues02
//          val reJson02 = replaceValues02(site_jsonData, updatedReadings, stationId, updatedTask1Data)
//          //reJson02调用别的方法
////          println(reJson02)
//        }
//
//      } else {
//        println("所有字段都已存在，无需反查数据库")
//        //根据map2集合和iotdb数据组合替代
//        val site_jsonData: IndexedSeq[JsValue] = jsonData02.value.filter { jsonObj =>
//          val deviceName = (jsonObj \ "deviceName").asOpt[String]
//          deviceName.exists(keysFromMap2.contains)
//        }
//
//        if(site_jsonData.size>1000){
//          //开始组合json往Python那边发送
//          val rebatchjsons = site_jsonData.grouped(500).toList
//          for (rebatchjson <- rebatchjsons) {
//            // 确保将整个 updatedTask2Data 传入 replaceValues02
//            val reJson02 = replaceValues02(rebatchjson, readings2, stationId, updatedTask1Data)
//            //reJson02调用别的方法
//          }
//        }else{
//          // 确保将整个 updatedTask2Data 传入 replaceValues02
//          val reJson02 = replaceValues02(site_jsonData, readings2, stationId, updatedTask1Data)
//          //reJson02调用别的方法
//        }
//      }
      updatedTask1Data = Map.empty
    } else {
      // 大于300的按300一个批次来进行
      val batches = fields.grouped(2500).toList

      for (batch <- batches) {
        val jsonArray = jsonData.value

        // 提取与当前 batch 字段相关的 JSON 数据
        val relevantJsonArray = jsonArray.filter { jsonObj =>
          val requiredFields = List("nbActPower", "zcVField", "zcIField")
          requiredFields.forall(field => (jsonObj \ field).asOpt[String].exists(batch.contains))
        }

        val batchJson: JsArray = JsArray(relevantJsonArray)

        // 获取任务一的数据
        val task1Data = convertToTask1Data(batchJson, stationId)


        // 准备并发送SQL查询

        // 循环执行 5 次
        for ((offset, limit) <- queryRanges) {
          val query = getSqlQuery(lastTsOpt, batch.mkString(","), station,offset, limit)
          val current01 = LocalDateTime.now()
          val formattedDateTime01 = current01.format(formatter)
          // 打印格式化后的时间
          println(s"当前查询iotdb 300之前的时间: $formattedDateTime01")
          val response = sendRequest(query)
          val current04 = LocalDateTime.now()
          println(s"2500批次请求结束时间${current04.format(formatter)}")
          val batchReadings = handleResponse(response, stationId)

          val current02 = LocalDateTime.now()
          val formattedDateTime02 = current02.format(formatter)
          // 打印格式化后的时间
          println(s"当前查询iotdb 300之后的时间: $formattedDateTime02")
          // 计算功率数据，并更新到 updatedTask2Data 中
          val updatedTask2Data: Map[String, Map[String,  List[Any]]] = calculatePower(task1Data, batchReadings, stationId)
//          if(updatedTask1Data.isEmpty){
//            updatedTask1Data=updatedTask2Data
//          }else{
//            updatedTask1Data = mergeMaps(updatedTask1Data, updatedTask2Data)
//          }
          //将功率的数据写入到数据库里边
          //插入iotdb
          val current05 = LocalDateTime.now()
          println(s"当前查询iotdb 300插入之前的时间${current05.format(formatter)}")
          convertToIoTDBJson(updatedTask2Data, stationID = stationId, task2Data = task3Data)
          val current03 = LocalDateTime.now()
          println(s"当前查询iotdb 300插入之后的时间: ${current03.format(formatter)}")
        }


        // 从updateTask2Data里边找到json2里边对应的状态值进行拼接(也可以从rockDB里边提取数据)
        // 从 map2 中获取 stationID 对应的键（排除 "timestamp"）
//        val keysFromMap2 = updatedTask1Data.get(stationId) match {
//          case Some(stationData) =>
//            stationData.keys.filter(_ != "timestamp").toList
//          case None =>
//            List.empty[String]
//        }
//        // 从 map1 中根据 keysFromMap2 获取对应的字段值
//        val fieldMappings = statMap1(stationId)
//        val json2List = keysFromMap2
//          .flatMap(key => fieldMappings.get(key).toList) // 获取对应键的字段值
//          .flatMap { case (a, b) => List(a, b) } // 展开字段对
//          .distinct // 去重
//        //查看取出的值在里边是否存在 不存在就反查数据库，存在就不反查
//
//        readings2=IoTDBReadingData(None.getOrElse(""), Map.empty[String, List[Option[Any]]])
//        // 检查 json2List 中的字段是否存在于 IoTDBReadingData 的 values 中
//        val missingFields = json2List.filterNot(readings2.values.contains)

        // 输出需要反查数据库的字段
//        println(s"需要反查数据库的字段: $missingFields")
//        if (missingFields.nonEmpty) {
//          val query = getSqlQuery(lastTsOpt, missingFields.mkString(","), station,0,1125)
//          val response = sendRequest(query)
//          val read_new: IoTDBReadingData = handleResponse(response, stationId)
//
//          // 合并原有数据和新的查询结果
//          // 如果 readings2 为空，则直接使用 read_new；否则进行合并
//          val updatedReadings = if (readings2.values.isEmpty) {
//            read_new
//          } else {
//            // 合并原有数据和新的查询结果
//            val updatedValues = readings2.values ++ read_new.values
//            readings2.copy(values = updatedValues)
//          }
//
//          //从json里边选出对应的设备名称，然后再替换往下游输出
//          val site_jsonData: IndexedSeq[JsValue] = jsonData02.value.filter { jsonObj =>
//            val deviceName = (jsonObj \ "deviceName").asOpt[String]
//            deviceName.exists(keysFromMap2.contains)
//          }
//
//          if(site_jsonData.size>1000){
//            //开始组合json往Python那边发送
//            val rebatchjsons = site_jsonData.grouped(1000).toList
//            for (rebatchjson <- rebatchjsons) {
//              // 确保将整个 updatedTask2Data 传入 replaceValues02
//              val reJson02 = replaceValues02(rebatchjson, updatedReadings, stationId, updatedTask1Data)
//              //reJson02调用别的方法
//              println(reJson02)
//            }
//          }else{
//            // 确保将整个 updatedTask2Data 传入 replaceValues02
//            val reJson02 = replaceValues02(site_jsonData, updatedReadings, stationId, updatedTask1Data)
//            //reJson02调用别的方法
////            println(reJson02)
//          }
//
//        } else {
//          println("所有字段都已存在，无需反查数据库")
//          //从json里边选出对应的设备名称，然后再替换往下游输出
//          val site_jsonData: IndexedSeq[JsValue] = jsonData02.value.filter { jsonObj =>
//            val deviceName = (jsonObj \ "deviceName").asOpt[String]
//            deviceName.exists(keysFromMap2.contains)
//          }
//
//          if(site_jsonData.size>1000){
//            //开始组合json往Python那边发送
//            val rebatchjsons = site_jsonData.grouped(1000).toList
//            for (rebatchjson <- rebatchjsons) {
//              // 确保将整个 updatedTask2Data 传入 replaceValues02
//              val reJson02 = replaceValues02(rebatchjson, readings2, stationId, updatedTask1Data)
//              //reJson02调用别的方法
////              println(reJson02)
//            }
//          }else{
//            // 确保将整个 updatedTask2Data 传入 replaceValues02
//            val reJson02 = replaceValues02(site_jsonData, readings2, stationId, updatedTask1Data)
//            //reJson02调用别的方法
////            println(reJson02)
//          }
//        }


      }
    }
    //可以先获取，然后里边需要的json,就删掉，存储新的
//    updatedTask1Data = Map.empty
    val current = LocalDateTime.now()
    // 定义日期时间格式

    // 格式化当前时间
    val formattedDateTime = current.format(formatter)
    // 打印格式化后的时间
  }

  def mergeMaps(map1: Map[String, Map[String, List[Any]]], map2: Map[String, Map[String, List[Any]]]): Map[String, Map[String, List[Any]]] = {
    (map1.keySet ++ map2.keySet).foldLeft(Map[String, Map[String, List[Any]]]()) { (acc, key) =>
      val combinedInnerMap = (map1.getOrElse(key, Map()), map2.getOrElse(key, Map())) match {
        case (innerMap1, innerMap2) =>
          // 合并内层 Map
          (innerMap1.keySet ++ innerMap2.keySet).foldLeft(Map[String, List[Any]]()) { (innerAcc, innerKey) =>
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

  def extractTask3Data(json: JsValue): Map[String, Map[String, (String,String)]] = {
    (json \ "data").asOpt[JsArray] match {
      case Some(dataArray) =>
        dataArray.value.flatMap { dataObj =>
          val stationId = (dataObj \ "stationId").as[String]
          val zcPointList = (dataObj \ "zcPointList").as[JsArray]
          val deviceMap = zcPointList.value.map { zcPoint =>
            val deviceName = (zcPoint \ "deviceName").as[String]
            val nbqStatus = (zcPoint \ "nbqStatus").as[String] // 功率字段
            val nbRunningStatus = (zcPoint \ "nbRunningStatus").as[String] // 功率字段
            deviceName -> (nbqStatus,nbRunningStatus)
          }.toMap
          List(stationId -> deviceMap)
        }.toMap
      case None => Map.empty
    }
  }

  def convertToIoTDBJson02(data: Map[String, Map[String, List[Any]]], stationID: String, task2Data: Map[String, Map[String, String]]): Unit = {
    // 提取所有的时间戳
    val timestamps: List[Long] = data.get(stationID).flatMap(_.get("timestamp").map(_.asInstanceOf[List[Long]])).getOrElse(List.empty)

//    println(s"打印字符串时间戳：$timestamps")
    // 获取任务映射
    try {
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
          "timestamps" -> JsArray(timestamps.map(t => JsNumber(t.toDouble))),
          "measurements_list" -> JsArray(measurementsList.map(list => JsArray(list.map(JsString)))),
          "data_types_list" -> JsArray(dataTypesList.map(list => JsArray(list.map(JsString)))),
          "values_list" -> JsArray(valuesList.map(list => JsArray(list.map(JsNumber(_))))),
          "is_aligned" -> false,
          "devices" -> JsArray(devices.map(JsString))
        )


      val current05 = LocalDateTime.now()
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      println(s"当前iotdb里边插入之前的时间${current05.format(formatter)}")
        // 将 JSON 对象转换为字符串
      sendToIoTDB(Json.stringify(json))
        // 处理转换后的 Long 类型时间戳
        println("能正常转换")
    } catch {
      case e: NumberFormatException =>
        // 处理转换失败的情况
        println(s"无法将字符转换为 Long 类型: ${e.getMessage}")
    }
  }


  def convertToIoTDBJson(data: Map[String, Map[String, List[Any]]], stationID: String, task2Data: Map[String, Map[String, String]]): Unit = {
    // 缓存 stationData 避免多次查找
    val stationDataOpt = data.get(stationID)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    // 提取所有的时间戳
    val timestamps: ListBuffer[Long] = stationDataOpt.flatMap(_.get("timestamp"))
      .map(_.collect {
        case num: Long => num
        case str: String =>
          try {
            str.toLong
          } catch {
            case _: NumberFormatException => 0L
          }
      }.to(ListBuffer.canBuildFrom))
      .getOrElse(ListBuffer.empty)

    try {
      // 缓存 taskMap 避免多次查找
      val taskMap: Option[Map[String, String]] = task2Data.get(stationID)
      // 获取 taskMap 中所有的目标测量名称
      val existingMeasurementNames = taskMap.map(_.values.toSet).getOrElse(Set.empty)

      // 过滤掉时间戳和 taskMap 中已有的测量名称
      val validMeasurements = stationDataOpt.map { stationData =>
        stationData.filterKeys(key => key != "timestamp" && !existingMeasurementNames.contains(key)).keys.toList
      }.getOrElse(List.empty)

      val current03 = LocalDateTime.now()
      println(s"当前iotdb里边插入z转换之前的时间：${current03.format(formatter)}")
      //得到设备名称并不是里边的值
      // 使用 validMeasurements 获取 taskMap 里边对应的值
      val measurementNames = validMeasurements.map { measurementName =>
        taskMap.flatMap(_.get(measurementName)).getOrElse(measurementName)
      }
      //得到列值长度
      val dataTypeNames = List.fill(measurementNames.size)("FLOAT") // 假设数据类型为 FLOAT
      println(s"打印字段长度：${dataTypeNames.size}")
      // 构建 measurements_list, data_types_list, 和 values_list
      val (measurementsList, dataTypesList, valuesList) = {

        val valueLists = timestamps.zipWithIndex.map { case (_, index) =>
          val buffer = ListBuffer[Double]()
          measurementNames.foreach { measurementName =>
            stationDataOpt.flatMap(_.get(measurementName)) match {
              case Some(values) if index < values.length =>
                values(index) match {
                  case num: Double => buffer += num
                  case num: Float => buffer += num.toDouble
                  case _ => buffer += null.asInstanceOf[Double]
                }
              case _ => buffer += null.asInstanceOf[Double]
            }
          }
          buffer.toList
        }


        (List.fill(timestamps.size)(measurementNames),
          List.fill(timestamps.size)(dataTypeNames),
          valueLists)
      }

      val current04 = LocalDateTime.now()
      println(s"当前iotdb里边插入z转换之后的时间${current04.format(formatter)}")
      // 设备名称
      val devices: List[String] = List.fill(timestamps.size)(s"root.ln.`$stationID`")

      // 构建最终的 JSON 对象
      // 构建最终的 JSON 对象
      val timestampsJson = JsArray(timestamps.map(t => JsNumber(t.toDouble)))
      val measurementsListJson = JsArray(measurementsList.map(list => JsArray(list.map(JsString))))
      val dataTypesListJson = JsArray(dataTypesList.map(list => JsArray(list.map(JsString))))
      val valuesListJson = JsArray(valuesList.map(list => JsArray(list.map(JsNumber(_)))))
      val devicesJson = JsArray(devices.map(JsString))

      val json = Json.obj(
        "timestamps" -> timestampsJson,
        "measurements_list" -> measurementsListJson,
        "data_types_list" -> dataTypesListJson,
        "values_list" -> valuesListJson,
        "is_aligned" -> false,
        "devices" -> devicesJson
      )

      val current05 = LocalDateTime.now()
      println(s"当前iotdb里边插入之前的时间${current05.format(formatter)}")
      // 将 JSON 对象转换为字符串
      sendToIoTDB(Json.stringify(json))
      // 处理转换后的 Long 类型时间戳
      println("能正常转换")
    } catch {
      case e: NumberFormatException =>
        // 处理转换失败的情况
        println(s"无法将字符转换为 Long 类型: ${e.getMessage}")
    }
  }


  // 这里假设 sendToIoTDB 方法已经定义
  def sendToIoTDB(data: String): Unit = {
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