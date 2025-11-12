package com.keystar.flink.diagnos_table


import play.api.libs.json.{Format, Json}

import java.io.File
import java.sql.{Connection, DriverManager, Statement}
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

// 输出数据项 case class
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
                       level: String,
                       duration_status: Boolean,
                       duration_seconds: Int,
                       description: String
                     )

// 规则主体 case class
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

object JsonFormats {
  implicit val inputDataFormat: Format[InputData] = Json.format[InputData]
  implicit val outputDataFormat: Format[OutputData] = Json.format[OutputData]
  implicit val ruleFormat: Format[Rule] = Json.format[Rule]

  // RuleWrapper 需要包含对 Rule 的引用
  case class RuleWrapper(rule: Rule)
  implicit val ruleWrapperFormat: Format[RuleWrapper] = Json.format[RuleWrapper]
}

object DeleteIotDB {
  def main(args: Array[String]): Unit = {

    val folderPath: String = "F:\\test\\data03"
    loadRules02(folderPath)
  }

  private def loadRules02(folderPath:String): Unit = {
    val iotdbUrl = "jdbc:iotdb://172.16.1.35:6667"
    val iotdbUser = "root"
    val iotdbPassword = "root"
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver")
    val iotdbConn: Connection = DriverManager.getConnection(iotdbUrl, iotdbUser, iotdbPassword)

    val folder = new File(folderPath)
    if (folder.exists() && folder.isDirectory) {
      folder.listFiles().foreach { file =>
        if (file.isFile && file.getName.endsWith(".json")) {
          try {
            // 读取文件内容并解析为 JSON
            val jsonStr = scala.io.Source.fromFile(file).mkString
            val jsonData = Json.parse(jsonStr)

            // 使用 RuleWrapper 解析 JSON 数据
            import JsonFormats._
            val ruleWrapper = jsonData.as[RuleWrapper]

            // 构建 table -> Seq[output] 的映射
            val tableToOutputs = ruleWrapper.rule.output_data
              .groupBy(_.table) // 按 table 分组
              .map { case (table, outputs) =>
                table -> outputs.map(_.position).toList // 提取 output 并转换为 List
              }
            //循环每个map然后执行删除操作
            //拼接drop timeseries root.ln.`1523723612516384521`.B7_25_isGridSideFreqHigh; 然后进行删除
            tableToOutputs.foreach { case (table, positions) =>
              deleteTimeSeries(iotdbConn, table, positions)
            }
          } catch {
            case ex: Exception =>
              println(s"[Error] Failed to process file ${file.getAbsolutePath}: ${ex.getMessage}")
          }
        }
      }
    }
  }
  def deleteTimeSeries(connection: Connection, deviceId: String, positions: List[String]): Unit = {
    if (positions.isEmpty) {
      println(s"[Info] No positions to delete for device: $deviceId")
      return
    }

    var statement: Statement = null
    try {
      statement = connection.createStatement()

      // 计算总点位数量和批次大小
      val totalPositions = positions.size
      val batchSize = 1 // 每批处理的数量
      var successCount = 0
      var failedCount = 0

      println(s"[Info] Starting to delete ${totalPositions} time series for device: $deviceId")

      // 分批处理点位
      positions.grouped(batchSize).zipWithIndex.foreach { case (batch, batchIndex) =>
        batch.foreach { position =>
          val fullPath = s"$deviceId.$position"
          try {
            val sql = s"DROP TIMESERIES $fullPath"
            println(sql)
            statement.executeUpdate(sql)
            successCount += 1
            if (successCount % 100 == 0) {
              println(s"[Info] Deleted $successCount time series so far")
            }
          } catch {
            case ex: Exception =>
              failedCount += 1
              println(s"[Error] Failed to delete time series: $fullPath - ${ex.getMessage}")
          }
        }

        println(s"[Info] Batch ${batchIndex + 1}/${math.ceil(totalPositions.toDouble / batchSize).toInt} completed")
      }

      println(s"[Info] Device $deviceId: Deleted $successCount time series, failed $failedCount")

    } catch {
      case ex: Exception =>
        println(s"[Error] Failed to delete time series for device $deviceId: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      if (statement != null) {
        try {
          statement.close()
        } catch {
          case ex: Exception =>
            println(s"[Error] Failed to close statement: ${ex.getMessage}")
        }
      }
    }
  }
}
