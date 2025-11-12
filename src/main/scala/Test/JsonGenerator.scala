package Test

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.io.Source


// 定义原始 JSON 数据的 case class
case class OriginalData(code: String, msg: String, data: List[StationData])
case class StationData(stationId: String, totalRadiation: String, zcNum: Int, offNum: Int, zcPointList: List[ZcPoint])
case class ZcPoint(deviceName: String, nbqStatus: Option[String], nbActPower: String, zcVField: String, zcIField: String, zcPField: String)

// 定义新 JSON 数据的 case class
case class Rule(
                 `type`: String,
                 name: String,
                 desc: String,
                 alarm: Boolean,
                 level: String,
                 expression: List[String],
                 script: List[String],
                 period: List[Int],
                 input_data: List[InputData],
                 output_data: List[OutputData]
               )
case class InputData(
                      attr: String,
                      source: String,
                      table: String,
                      template: String,
                      position: String,
                      properties: String,
                      unit: String,
                      accuracy: String,
                      `type`: String,
                      value: String
                    )
case class OutputData(
                       output: String,
                       source: String,
                       table: String,
                       template: String,
                       position: String,
                       properties: String,
                       unit: String,
                       accuracy: String,
                       `type`: String,
                       value: String,
                       alarm: Boolean,
                       level: String
                     )
case class NewJson(rule: Rule)

object OriginalData {
  // ZcPoint 的解码器
  implicit val zcPointDecoder: Decoder[ZcPoint] = Decoder.forProduct6(
    "deviceName", "nbqStatus", "nbActPower", "zcVField", "zcIField", "zcPField"
  )(ZcPoint.apply)

  // StationData 的解码器
  implicit val stationDataDecoder: Decoder[StationData] = Decoder.forProduct5(
    "stationId", "totalRadiation", "zcNum", "offNum", "zcPointList"
  )(StationData.apply)

  // OriginalData 的解码器
  implicit val originalDataDecoder: Decoder[OriginalData] = Decoder.forProduct3(
    "code", "msg", "data"
  )(OriginalData.apply)
}

object JsonGenerator {
  def main(args: Array[String]): Unit = {
    val originalJson02 =
      """
        |{
        |	"code": "200",
        |	"msg": "操作成功",
        |	"data": [{
        |			"stationId": "1816341324371066880",
        |			"totalRadiation": "WM01_YC0013",
        |			"zcNum": 8251,
        |			"offNum": 841,
        |			"zcPointList": [{
        |					"deviceName": "FZ030-NB07-ZC12",
        |					"nbqStatus": null,
        |					"nbActPower": "I030_7_INVERTER_ActPower",
        |					"zcVField": "I030_7_INVERTER_Line12_U",
        |					"zcIField": "I030_7_INVERTER_Line12_I",
        |					"zcPField": "I030_7_INVERTER_Line12_P"
        |				},
        |				{
        |					"deviceName": "FZ030-NB07-ZC13",
        |					"nbqStatus": null,
        |					"nbActPower": "I030_7_INVERTER_ActPower",
        |					"zcVField": "I030_7_INVERTER_Line13_U",
        |					"zcIField": "I030_7_INVERTER_Line13_I",
        |					"zcPField": "I030_7_INVERTER_Line13_P"
        |				}
        |			]
        |		},
        |		{
        |			"stationId": "1816341324371066882",
        |			"totalRadiation": "WM01_YC0013",
        |			"zcNum": 8251,
        |			"offNum": 841,
        |			"zcPointList": [{
        |					"deviceName": "FZ030-NB07-ZC12",
        |					"nbqStatus": null,
        |					"nbActPower": "I030_7_INVERTER_ActPower",
        |					"zcVField": "I030_7_INVERTER_Line12_U",
        |					"zcIField": "I030_7_INVERTER_Line12_I",
        |					"zcPField": "I030_7_INVERTER_Line12_P"
        |				},
        |				{
        |					"deviceName": "FZ030-NB07-ZC13",
        |					"nbqStatus": null,
        |					"nbActPower": "I030_7_INVERTER_ActPower",
        |					"zcVField": "I030_7_INVERTER_Line13_U",
        |					"zcIField": "I030_7_INVERTER_Line13_I",
        |					"zcPField": "I030_7_INVERTER_Line13_P"
        |				}
        |			]
        |		}
        |	]
        |}
      """.stripMargin

    val filePath = "src/main/scala/com/keystar/flink/kr_cache_demo/组串功率入库.json"
    // 读取文件内容
    /**
     * 本次在当前的任务范围内，可以暂不考虑（但是后续会涉及）
     */

    val originalJson = Source.fromFile(filePath).mkString
    import OriginalData._

    // 解析原始 JSON
    val originalData = decode[OriginalData](originalJson) match {
      case Right(data) => data
      case Left(error) => throw new RuntimeException(s"Failed to decode JSON: ${error.getMessage}")
    }

    // 生成新的 JSON 数据
    val newJson = generateNewJson(originalData)

//    println(s"打印数据：$newJson")
    // 将新的 JSON 数据写入文件
    writeJsonToFile(newJson.asJson.spaces2, "output.json")
  }

  def generateNewJson(originalData: OriginalData): NewJson = {
    // 遍历所有站点数据，生成规则
    val allRules = originalData.data.flatMap { stationData =>
      val stationId = stationData.stationId
      val zcPointList = stationData.zcPointList

      zcPointList.map { zcPoint =>
        val (attrI, attrU, attrP) = createAttributes(zcPoint, stationId)
        val expression = s"/$attrI * /$attrU"

        // 输入数据
        val inputDataI = createInputData(attrI, zcPoint.zcIField, s"root.ln.`$stationId`",zcPoint.deviceName)
        val inputDataU = createInputData(attrU, zcPoint.zcVField, s"root.ln.`$stationId`",zcPoint.deviceName)

        // 输出数据
        val outputData = createOutputData(expression, zcPoint.zcPField, s"root.ln.`$stationId`",zcPoint.deviceName)

        // 创建单个规则
        Rule(
          `type` = "功率计算规则",
          name = s"${zcPoint.deviceName}_功率计算规则_$stationId",
          desc = s"计算设备 ${zcPoint.deviceName} 的功率 (站点ID: $stationId)",
          alarm = true,
          level = "warning",
          expression = List(expression),
          script = List("getInvRiseDaily"),
          period = List(10),
          input_data = List(inputDataI, inputDataU),
          output_data = List(outputData)
        )
      }
    }

    // 合并所有规则
    val combinedRule = Rule(
      `type` = "合并功率计算规则",
      name = "合并所有设备的功率计算规则",
      desc = "合并所有设备的功率计算规则",
      alarm = true,
      level = "warning",
      expression = allRules.flatMap(_.expression),
      script = allRules.flatMap(_.script).distinct,
      period = allRules.flatMap(_.period).distinct,
      input_data = allRules.flatMap(_.input_data),
      output_data = allRules.flatMap(_.output_data)
    )

    // 返回最终的新 JSON 数据
    NewJson(combinedRule)
  }

  // 创建属性名称（添加 stationId 到 attr）
  private def createAttributes(zcPoint: ZcPoint, stationId: String): (String, String, String) = {
    val attrI = s"attr_${zcPoint.zcIField}_$stationId"
    val attrU = s"attr_${zcPoint.zcVField}_$stationId"
    val attrP = s"attr_${zcPoint.zcPField}_$stationId"
    (attrI, attrU, attrP)
  }

  // 创建输入数据
  private def createInputData(attr: String, position: String, table: String,deviceName:String): InputData = {
    InputData(
      attr = attr,
      source = "iotdb",
      table = table,
      template = "01",
      position = position,
      properties = deviceName,
      unit = "单位",
      accuracy = "0.1",
      `type` = "float",
      value = ""
    )
  }

  // 创建输出数据
  private def createOutputData(expression: String, position: String, table: String,deviceName:String): OutputData = {
    OutputData(
      output = expression,
      source = "iotdb",
      table = table,
      template = "02",
      position = s"${position}",
      properties = deviceName,
      unit = "(单位)",
      accuracy = "0.1",
      `type` = "float",
      value = "10",
      alarm = true,
      level = "warning"
    )
  }

  // 将 JSON 数据写入文件
  private def writeJsonToFile(jsonString: String, filePath: String): Unit = {
    try {
      // 使用 UTF-8 编码写入文件
      Files.write(
        Paths.get(filePath),
        jsonString.getBytes("UTF-8"),
        StandardOpenOption.CREATE, // 如果文件不存在则创建
        StandardOpenOption.TRUNCATE_EXISTING // 如果文件存在则覆盖
      )
    } catch {
      case e: Exception => throw new RuntimeException(s"Failed to write JSON to file: ${e.getMessage}")
    }
  }
}