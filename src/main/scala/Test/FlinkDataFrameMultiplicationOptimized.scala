//package Test
//
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.tuple.Tuple2
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api._
//import play.api.libs.json._
//import scala.collection.mutable
//import java.net.HttpURLConnection
//import java.net.URL
//
//object FlinkDataFrameMultiplicationOptimized {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // 设置并行度
//    env.setParallelism(4)
//    //    val tableEnv = StreamTableEnvironment.create(env)
//
//
//    // 模拟从 IoTDB 获取的 JSON 数据
//    val iotdbJsonData =
//      """{
//        |    "expressions": [
//        |        "root.ln.`1816341324371066880`.I10_5_INVERTER_Line04_U",
//        |        "root.ln.`1816341324371066880`.I10_5_INVERTER_Line04_I",
//        |        "root.ln.`1816341324371066880`.I10_5_INVERTER_Line03_I",
//        |        "root.ln.`1816341324371066880`.I10_5_INVERTER_Line03_U"
//        |    ],
//        |    "column_names": null,
//        |    "timestamps": [
//        |        1723092345000,
//        |        1723092360000,
//        |        1723092375000
//        |    ],
//        |    "values": [
//        |        [553.59],
//        |        [553.59],
//        |        [457.67],
//        |        [451.17]
//        |    ]
//        |}""".stripMargin
//
//    // 模拟点位关系的 JSON 数据
//    val pointRelationJsonData =
//      """{"code":"200","msg":"操作成功","data":[
//        |    {"stationId":"1816341324371066880","totalRadiation":"WM01_YC0013","zcNum":8251,"offNum":841,"zcPointList":  [
//        |        {"deviceName":"FZ030-NB07-ZC12","nbqStatus":null,"nbActPower":"I030_7_INVERTER_ActPower","zcVField":"I10_5_INVERTER_Line04_U","zcIField":"I10_5_INVERTER_Line04_I","zcPField":"I10_5_INVERTER_Line04_P"},
//        |        {"deviceName":"FZ030-NB07-ZC12","nbqStatus":null,"nbActPower":"I030_7_INVERTER_ActPower","zcVField":"I10_5_INVERTER_Line03_U","zcIField":"I10_5_INVERTER_Line03_I","zcPField":"I10_5_INVERTER_Line03_P"}
//        |    ]}
//        |]}""".stripMargin
//    //
//    //    // 解析 IoTDB JSON 数据为 DataStream
//    //    val iotdbDataStream = env.fromElements(iotdbJsonData)
//    //      .flatMap(new MapFunction[String, (Long, mutable.Map[String, Double])] {
//    //        override def map(value: String): (Long, mutable.Map[String, Double]) = {
//    //          val jsonObj = Json.parse(value).as[JsObject]
//    //          val timestamps = (jsonObj \ "timestamps").as[List[Long]]
//    //          val expressions = (jsonObj \ "expressions").as[List[String]]
//    //          val values = (jsonObj \ "values").as[List[List[JsValue]]]
//    //          val dataMap = mutable.Map[String, Double]()
//    //          for (i <- expressions.indices) {
//    //            val key = expressions(i).split("\\.").last
//    //            dataMap(key) = values(i)(0).as[Double]
//    //          }
//    //          (timestamps.head, dataMap)
//    //        }
//    //      })
//    //
//    //    // 解析点位关系 JSON 数据为 DataStream
//    //    val pointRelationDataStream = env.fromElements(pointRelationJsonData)
//    //      .flatMap(new MapFunction[String, (String, String, String, String)] {
//    //        override def map(value: String): (String, String, String, String) = {
//    //          val jsonObj = Json.parse(value).as[JsObject]
//    //          val data = (jsonObj \ "data").as[JsArray].head.as[JsObject]
//    //          val zcPointList = (data \ "zcPointList").as[JsArray]
//    //          val result = mutable.ListBuffer[(String, String, String, String)]()
//    //          for (point <- zcPointList) {
//    //            val pointObj = point.as[JsObject]
//    //            val zcVField = (pointObj \ "zcVField").as[String]
//    //            val zcIField = (pointObj \ "zcIField").as[String]
//    //            val zcPField = (pointObj \ "zcPField").as[String]
//    //            result += ((zcVField, zcIField, zcPField, (data \ "stationId").as[String]))
//    //          }
//    //          result.head
//    //        }
//    //      })
//    //
//    //    // 将 DataStream 转换为 DataFrame
//    //    val iotdbTable = tableEnv.fromDataStream(iotdbDataStream)
//    //      .as("timestamp", "dataMap")
//    //
//    //    val pointRelationTable = tableEnv.fromDataStream(pointRelationDataStream)
//    //      .as("zcVField", "zcIField", "zcPField", "stationId")
//    //
//    //    // 注册 UDF 用于计算功率
//    //    tableEnv.createTemporarySystemFunction("calculatePower", new CalculatePowerUDF)
//    //
//    //    // 关联并计算新点位数据
//    //    val resultTable = tableEnv.sqlQuery(
//    //      s"""
//    //         |SELECT
//    //         |  t1.timestamp,
//    //         |  t2.zcPField,
//    //         |  calculatePower(t1.dataMap, t2.zcVField, t2.zcIField) AS powerValue
//    //         |FROM
//    //         |  $iotdbTable t1
//    //         |JOIN
//    //         |  $pointRelationTable t2
//    //         |ON true
//    //       """.stripMargin
//    //    )
//    //
//    //    // 将结果转换为符合 IoTDB 写入要求的 JSON 并写入
//    //    val resultStream = tableEnv.toDataStream(resultTable)
//    //    resultStream.map(new MapFunction[Row, String] {
//    //      override def map(value: Row): String = {
//    //        val timestamp = value.getField(0).asInstanceOf[Long]
//    //        val zcPField = value.getField(1).asInstanceOf[String]
//    //        val powerValue = value.getField(2).asInstanceOf[Double]
//    //
//    //        val timestamps = List(timestamp)
//    //        val measurementsList = List(List(zcPField))
//    //        val dataTypesList = List(List("DOUBLE"))
//    //        val valuesList = List(List(powerValue))
//    //        val devices = List(s"root.ln.`1816341324371066880`")
//    //
//    //        val json = Json.obj(
//    //          "timestamps" -> JsArray(timestamps.map(t => JsNumber(t.toDouble))),
//    //          "measurements_list" -> JsArray(measurementsList.map(list => JsArray(list.map(JsString)))),
//    //          "data_types_list" -> JsArray(dataTypesList.map(list => JsArray(list.map(JsString)))),
//    //          "values_list" -> JsArray(valuesList.map(list => JsArray(list.map(JsNumber(_))))),
//    //          "is_aligned" -> false,
//    //          "devices" -> JsArray(devices.map(JsString))
//    //        )
//    //
//    //        val jsonString = Json.stringify(json)
//    //        writeToIoTDB(jsonString)
//    //        jsonString
//    //      }
//    //    }).print()
//    //
//    //    // 执行 Flink 作业
//    //    env.execute("Flink DataFrame Multiplication Job")
//    //  }
//    //
//    //  def writeToIoTDB(jsonString: String): Unit = {
//    //    val url = new URL("http://your-iotdb-server:port/api/v1/insert")
//    //    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
//    //    connection.setRequestMethod("POST")
//    //    connection.setRequestProperty("Content-Type", "application/json")
//    //    connection.setDoOutput(true)
//    //
//    //    val outputStream = connection.getOutputStream()
//    //    outputStream.write(jsonString.getBytes())
//    //    outputStream.flush()
//    //    outputStream.close()
//    //
//    //    val responseCode = connection.getResponseCode()
//    //    if (responseCode == HttpURLConnection.HTTP_OK) {
//    //      println("Data written to IoTDB successfully.")
//    //    } else {
//    //      println(s"Failed to write data to IoTDB. Response code: $responseCode")
//    //    }
//    //    connection.disconnect()
//    //  }
//    //}
//    //
//    //// 自定义 UDF 用于计算功率
//    //class CalculatePowerUDF extends ScalarFunction {
//    //  def eval(dataMap: mutable.Map[String, Double], zcVField: String, zcIField: String): Double = {
//    //    if (dataMap.contains(zcVField) && dataMap.contains(zcIField)) {
//    //      dataMap(zcVField) * dataMap(zcIField)
//    //    } else {
//    //      0.0
//    //    }
//    //  }
//  }
//}