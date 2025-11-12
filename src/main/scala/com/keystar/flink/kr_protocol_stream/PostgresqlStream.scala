package com.keystar.flink.kr_protocol_stream


import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.{Collector, OutputTag}

import scala.collection.mutable


object PostgresqlStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /*    env.setParallelism(4)*/
    env.setParallelism(1)
    // 创建数据源
    // 定义 Side Output 标签
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

//    println(s"打印时间戳：${timestamp}")

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


//    val sitePartitionMap: mutable.Map[String, Int] = mutable.Map(
//      "root.ln.`1821385107949223936`" -> 14,
//      "root.ln.`1816341324371066880`" -> 6,
//      "root.ln.`1269014857442188595`" -> 5,
//      "root.ln.`1821385428708622336`" -> 4,
//      "root.ln.`1801181094087753728`" -> 3,
//      "root.ln.`1821384526656438272`" -> 3,
//      "root.ln.`1821384763865300992`" -> 3,
//      "root.ln.`1269014857443188597`" -> 3,
//      "root.ln.`1521412352616268498`" -> 17, //木垒场站
//      "root.ln.`1521714652626283467`" -> 5,  //木垒
//      "root.ln.`1523723612516384521`" -> 7,  //木垒
//      "root.ln.`1522125219418180352`" -> 2
//    )



    val events:DataStreamSource[KrProtocolData] = env.addSource(new PostgresqlSource(site)).setParallelism(1)
//    events.print()


    val processedStream = events
      .map(new IncrementalMapper(sitePartitionMap))
      .returns(TypeInformation.of(classOf[Tuple2[String, KrProtocolData]])) // 显式指定返回类型
      .keyBy(_._1) // 使用虚拟键进行分组
      .process(new TransformPostgresql(timestamp,sitePartitionArgs.toMap)) // 使用自定义的 KeyedProcessFunction
      .setParallelism(22) // 设置并行度
    processedStream.print()
//    processedStream.addSink(new PostgresqlToMqSink).setParallelism(1)
    // 定义一个周期性的数据源，例如每分钟从IoTDB获取数据
    env.execute("Flink State Manager Test")
  }


  class IncrementalMapper(sitePartitionMap: mutable.Map[String, Int])
    extends MapFunction[KrProtocolData, (String, KrProtocolData)]
      with java.io.Serializable {

    // 为每个站点维护独立的计数器（避免不同站点互相干扰）
    private val siteCounters: mutable.Map[String, Long] = mutable.Map.empty

    override def map(value: KrProtocolData): (String, KrProtocolData) = {
      val siteId = value.iot_table.getOrElse("0")
      val numPartitions = sitePartitionMap.getOrElse(siteId, 1)

      // 获取或初始化站点专属计数器
      val counter = siteCounters.getOrElseUpdate(siteId, 0L)
      siteCounters(siteId) = counter + 1 // 先使用旧值，再递增

      val modValue = counter % numPartitions // 用旧值取模（确保第一个数据从0开始）
      val partitionKey = s"${siteId}_${modValue}"

      (partitionKey, value)
    }
  }
}
