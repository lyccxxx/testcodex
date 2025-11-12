package com.keystar.flink.flink_stream
import org.apache.flink.api.common.typeinfo.TypeInformation
import com.keystar.flink.iotdbfunction.{BatchProcessFunction, DiagnosisResult, TransformCoProcessFunction, TransformKeyProcessFunction}
import com.keystar.flink.iotdbstream.{DiagnosisRule, DiagnosisRuleSource, PeriodicSource, Test04}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.OutputTag
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.api.scala._  // 导入 Scala API 的隐式转换和类型工具
import scala.collection.mutable
import java.sql.{Connection, PreparedStatement}
object FlinkTest {
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
    val errorOutputTag = new OutputTag[DiagnosisResult]("error-points", TypeInformation.of(classOf[DiagnosisResult]))
    val events:DataStreamSource[String] = env.addSource(new DiagnosisRuleSource(sitePartitionMap)).setParallelism(1)
//    events.print()
    val keyedStream = events
      .map { str =>
        // 提取后缀作为 key 字段，保留原始字符串用于下游处理
        val parts = str.split("_")
        val key = if (parts.length > 1) parts.last else str  // 确保提取最后一部分
        (key, str)
      }.returns(createTypeInformation[(String, String)])
      .keyBy(_._1)  // 只根据 suffix 做 keyBy
      .process(new TransformKeyProcessFunction(timestamp, sitePartitionMap))  // 自定义的 ProcessFunction
      .setParallelism(6)

      keyedStream
        .keyBy((data:DiagnosisResult)=>data.expressions.dropRight(6))
        .process(new BatchProcessFunction).setParallelism(6)

      // 使用 JdbcSink 实现批量写入
//      val jdbcSink = JdbcSink.sink(
//          "INSERT INTO public.kr_single_date (id,iot_table,iot_fld,time) VALUES (?,?,?,?)", // SQL 插入语句
//          new JdbcStatementBuilder[DiagnosisResult] {
//            override def accept(ps: PreparedStatement, t: DiagnosisResult): Unit = {
//              // 设置 PreparedStatement 参数
//              ps.setLong(1, t.timestamp)
//              ps.setString(2, t.expressions.split("\\.").take(3).mkString("."))
//              ps.setString(3,t.expressions.split("\\.").lastOption.get)
//              ps.setLong(4,t.timestamp)
//            }
//          },
//          JdbcExecutionOptions.builder()
//            .withBatchSize(1000) // 批量大小
//            .withBatchIntervalMs(200) // 批量间隔时间
//            .withMaxRetries(3) // 最大重试次数
//            .build(),
//          new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//            .withUrl("jdbc:postgresql://192.168.5.12:5432/postgres")
//            .withDriverName("org.postgresql.Driver")
//            .withUsername("postgres")
//            .withPassword("123456")
//            .build()
//        )
//      errorStream.addSink(jdbcSink)
      env.execute("Flink State Manager Test")
  }
}