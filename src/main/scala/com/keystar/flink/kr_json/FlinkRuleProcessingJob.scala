package com.keystar.flink.kr_json
import com.keystar.flink.iotdbfunction.{BatchProcessFunction, DiagnosisResult}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import play.api.libs.json._

import scala.collection.mutable
import scala.io.Source
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple2

case class Rule(
                 `type`: String,
                 name: String,
                 desc: String,
                 alarm: Boolean,
                 level: String,
                 expression: Seq[String],
                 script: Seq[String],
                 period: Seq[Int],
                 input_data: Seq[InputData],
                 output_data: Seq[OutputData]
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
                       level: String,
                       duration_status: Boolean,
                       duration_seconds: Double,
                       description: String,
                       alg_desc: String,
                       alg_label_EN: String,
                       alg_brief_CH: String,
                       alg_param: Map[String, List[Double]], // 使用Map匹配JSON结构
                       alg_show_group: List[Group], // 直接使用Map匹配JSON对象
                       alg_show_levelcursor: List[String]
                     )

case class Group(group_title: String, group_line: List[String])

object JsonFormats {
  implicit val inputDataFormat: Format[InputData] = Json.format[InputData]
  implicit val groupFormat: Format[Group] = Json.format[Group]
  implicit val outputDataFormat: Format[OutputData] = Json.format[OutputData]
  implicit val ruleFormat: Format[Rule] = Json.format[Rule]

  // RuleWrapper 需要包含对 Rule 的引用
  case class RuleWrapper(rule: Rule)
  implicit val ruleWrapperFormat: Format[RuleWrapper] = Json.format[RuleWrapper]
}

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}

object FlinkRuleProcessingJob {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //获取传参数据
    val params = args.sliding(2, 2).collect {
      case Array(flag, value) if flag.startsWith("--") =>
        (flag.stripPrefix("--"), value)
    }.toMap

    // 获取参数，设置默认值
    val timestamp: Long = params.get("timestamp")
      .flatMap { str =>
        try Some(str.toLong)
        catch { case _: NumberFormatException => None }
      }
      .getOrElse(0L)


    val siteConfigStr = params.get("site").getOrElse("")
    val sitePartitionArgs = siteConfigStr.split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap { pair =>
        pair.split("=", 2) match {
          case Array(key, value) =>
            try Some(key -> value.toInt)
            catch { case _: NumberFormatException =>
              println(s"无效的分区值: $value，应为整数")
              None
            }
          case _ =>
            println(s"无效的站点分区参数格式: $pair，应使用 key=value 格式")
            None
        }
      }
    //    println(s"采集到的数据值：${sitePartitionArgs.toMap}")
    val deprecated: mutable.Map[String, Int] = mutable.Map()

    val sitePartitionMap: mutable.Map[String, Int]=deprecated++sitePartitionArgs.toMap

    val site=sitePartitionMap.keySet.toList

    println("打印数值情况：")
    println(s"集合和站点：${sitePartitionMap} ${site}")

    // 读取文件内容
    /**
     * 本次在当前的任务范围内，可以暂不考虑（但是后续会涉及）
     */
    val inputStream: DataStream[(String,InputData)] = env.addSource(new IoTDBSource(sitePartitionMap)).setParallelism(1)
    //需要将站点数据读取到缓存里边然后再进行其他的运算
    val periodicSource: DataStream[String] = env.addSource(new SitedataSource(sitePartitionMap)).setParallelism(3)
//    periodicSource.print()

//    inputStream.print()

    val vaildOutputTag = OutputTag[DiagnosisResult]("vaild-points");
    // 合并处理两个流
    val processedStream = periodicSource.connect(inputStream)
      .keyBy(
        (rule: String) => rule.split("_").last,
        (siteData: (String, InputData)) => siteData._1.split("_").last
      )
      .process(new RuleProcessor(vaildOutputTag, timestamp,sitePartitionMap))
      .setParallelism(8)



    val complexStream = periodicSource
      .keyBy(str => str.dropRight(1)) // 直接使用输入的字符串作为分区键
      .process(new RuleComplexProcessor(timestamp, sitePartitionMap)).setParallelism(2)


    //获取 vaild结尾的点位
    val vaildStream: DataStream[DiagnosisResult] = processedStream.getSideOutput(vaildOutputTag)

    vaildStream
      .keyBy((data:DiagnosisResult)=>data.expressions.dropRight(6))
      .process(new BatchProcessFunction).setParallelism(6)


    processedStream.keyBy((data:OutResult)=>data.station+"_"+data.properties.substring(0,2)).process(new IoTDBNumerSink).setParallelism(4)
    //再往下游进行输出
    val mergedStream = processedStream.union(complexStream)
    mergedStream.addSink(new IoTDBtoMqtt).setParallelism(1)

    env.execute("Real-time Rule Processing")
  }

  // 辅助方法：统一的键提取逻辑
  private def extractKey(fullKey: String): String = {
    val suffix = fullKey.split("_").last
    suffix
  }

  class IncrementalMapper(sitePartitionMap: mutable.Map[String, Int])
    extends MapFunction[InputData, (String, InputData)]
      with java.io.Serializable {

    // 为每个站点维护独立的计数器（避免不同站点互相干扰）
    private val siteCounters: mutable.Map[String, Long] = mutable.Map.empty

    override def map(value: InputData): (String, InputData) = {
      val siteId = value.table
      val numPartitions = sitePartitionMap.getOrElse(siteId, 1)

      // 获取或初始化站点专属计数器
      val counter = siteCounters.getOrElseUpdate(siteId, 0L)
      siteCounters(siteId) = counter + 1 // 先使用旧值，再递增


      val modValue = counter % numPartitions // 用旧值取模（确保第一个数据从0开始）
      val partitionKey = s"${siteId}_${modValue}"
      println(s"[DEBUG] siteId=$siteId, counter=$counter, numPartitions=$numPartitions, modValue=$modValue, key=$partitionKey")
      (partitionKey, value)
    }
  }
}
