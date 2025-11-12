package com.keystar.flink.kr_json


import com.keystar.flink.kr_protocol_stream.KrProtocolData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import play.api.libs.json.Json

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

// 定义 InputData 的结构（假设已定义）

class IoTDBSource(sitePartitionMap: mutable.Map[String, Int]) extends RichParallelSourceFunction[(String,InputData)] {

  private var isRunning: Boolean = true
  private var directoryPath: String = "F:\\data" // 目录路径
  private var directoryPath02: String = "/opt/data_json"
  private var lastReadTime: Long = System.currentTimeMillis() // 上次读取的时间戳
  private val processedFiles: mutable.Map[String, Long] = mutable.Map() // 已处理文件及其最后修改时间

  private val siteCounters: mutable.Map[String, Long] = mutable.Map.empty

  override def open(parameters: Configuration): Unit = {
    // 初始化时检查目录是否存在
    val directory = new File(directoryPath)
    if (!directory.exists() || !directory.isDirectory) {
      throw new RuntimeException(s"Directory not found or invalid: $directoryPath")
    }
  }

  override def run(ctx: SourceFunction.SourceContext[(String,InputData)]): Unit = {
      try {
        val directory = new File(directoryPath)
        val files = directory.listFiles().filter(_.isFile)
        //使用中间变量收集所有数据
        val allData = scala.collection.mutable.ListBuffer[(String, InputData)]()
        val siteSet = sitePartitionMap.keySet.toList
        files.foreach { file =>
          val fileName = file.getName
          val lastModified = file.lastModified()

          // 生成基于文件名的固定哈希值（使用文件名的哈希码）
          if (!processedFiles.contains(fileName) || processedFiles(fileName) < lastModified) {
            processedFiles(fileName) = lastModified

            val jsonStr = Source.fromFile(file).mkString
            val jsonData = Json.parse(jsonStr)

            import JsonFormats._
            val ruleWrapper = jsonData.as[RuleWrapper]


            val siteId = ruleWrapper.rule.input_data.headOption
              .map(_.table)
              .getOrElse("")

            val numPartitions = sitePartitionMap.getOrElse(siteId, 1)

            // 获取或初始化站点专属计数器
            val counter = siteCounters.getOrElseUpdate(siteId, 0L)
            siteCounters(siteId) = counter + 1 // 先使用旧值，再递增
            //      sitePartitionMap.put(siteId,counter+1L)
            val modValue = counter % numPartitions // 用旧值取模（确保第一个数据从0开始
            val partitionKey = generatePartitionKey(siteId, counter, siteSet)
//            println(s"打印文件名和长度11 key：${fileName} ${ruleWrapper.rule.input_data.size} ${partitionKey}")

            ruleWrapper.rule.input_data.foreach { data =>
              allData += ((partitionKey, data))
            }
          }
        }
        //所有文件处理完成后，统一发送给下游
        allData.foreach { case (key, data) =>
          ctx.collect((key, data))
        }
        Thread.sleep(1500) // 每 15 秒检查一次文件更新

      } catch {
        case e: Exception =>
          e.printStackTrace()
          Thread.sleep(5000)
      }
  }
  // 辅助方法：为特定设备的站点构建局部偏移量映射
  def buildDeviceSiteOffsets(deviceSites: List[String]): Map[String, Int] = {
    val deviceSitePartitionMap = sitePartitionMap.filterKeys(deviceSites.contains)
    var offset = 0
    deviceSitePartitionMap.toList.flatMap { case (siteId, numPartitions) =>
      val result = (siteId, offset)
      offset += numPartitions
      List(result)
    }.toMap
  }

  // 在类中添加一个辅助方法，用于统一生成分区键
  def generatePartitionKey(siteId: String, counter: Long, deviceSites: List[String]): String = {
    // 为当前设备的站点构建局部偏移量映射
    val deviceSiteOffsets = buildDeviceSiteOffsets(deviceSites)

    // 获取站点分区数
    val numPartitions = sitePartitionMap.getOrElse(siteId, 1)

    // 确保计数器不超过分区数
    val adjustedCounter = counter % numPartitions

    // 生成最终分区键
    val offset = deviceSiteOffsets.getOrElse(siteId, 0)
    s"${siteId}_${offset + adjustedCounter}"
  }


  override def cancel(): Unit = {
    isRunning = false
  }
}