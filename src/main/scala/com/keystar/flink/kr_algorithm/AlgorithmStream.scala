package com.keystar.flink.kr_algorithm

import com.keystar.flink.iotdbfunction.{BatchProcessFunction, DiagnosisResult}  // 确保导入AlgorithResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag
import scala.collection.mutable
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.scala.DataStream


object AlgorithmStream {
  // 隐式转换：将Java版SingleOutputStreamOperator转为Scala版DataStream
  implicit def javaOperatorToScalaStream[T](operator: SingleOutputStreamOperator[T]): DataStream[T] = {
    new DataStream[T](operator)
  }
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)                                      // 设置全局默认并行度为1

    // 解析参数（保持不变）
    // 解析命令行参数：将形如--key value的参数转为Map[String, String]
    val params = args.sliding(2, 2).collect {                   // sliding(2,2)：按每2个元素一组滑动（步长2）
      case Array(flag, value) if flag.startsWith("--") =>       // 匹配--key value格式
        (flag.stripPrefix("--"), value)                        // 去除--前缀，保留key和value
    }.toMap

    // 提取timestamp参数：尝试转为Long类型，默认0L
    val timestamp: Long = params.get("timestamp")
      .flatMap { str =>                                       // flatMap：过滤转换失败的情况（返回None）
        try Some(str.toLong) catch { case _: NumberFormatException => None }
      }
      .getOrElse(0L)     // 无参数时默认0

    // 解析site参数：格式为key=value,key2=value2（站点分区配置）
    val siteConfigStr = params.getOrElse("site", "")      // 无site参数时默认为空
    val sitePartitionArgs = siteConfigStr.split(",")      // 按逗号分割多个配置
      .map(_.trim)                                       // 去除空格
      .filter(_.nonEmpty)                                // 过滤空字符串
      .flatMap { pair =>                                 // 解析每个key=value对
        pair.split("=", 2) match {                      // 按等号分割（最多分割为2部分）
          case Array(key, value) =>
            try Some(key -> value.toInt) catch {        // 尝试将value转为Int
              case _: NumberFormatException =>
                println(s"无效的分区值: $value，应为整数")    // 转换失败时打印警告
                None
            }
          case _ =>                                        // 格式错误（无等号）
            println(s"无效的站点分区参数格式: $pair，应使用 key=value 格式")
            None
        }
      }


    // 将解析后的站点配置转为可变Map（方便后续修改）
    val sitePartitionMap: mutable.Map[String, Int] = mutable.Map(sitePartitionArgs: _*)
    println(s"${sitePartitionMap}")

    // 测输出流定义
    // 定义侧输出流标签：名称为"valid-points"，类型为DiagnosisResul
    // 关键调整1：显式声明Scala版OutputTag，并指定TypeInformation（解决类型推断问题）
    val validOutputTag: OutputTag[DiagnosisResult] =
      OutputTag[DiagnosisResult]("valid-points")(TypeInformation.of(classOf[DiagnosisResult]))

    // 添加数据源（保持不变）：从PostgreSQL读取数据（自定义PostgresqlSource方法读取）
    // 传入站点分区配置，并行度设为1（单线程读取）
    val sourceStream: DataStreamSource[String] = env.addSource(new PostgresqlSource(sitePartitionMap))
      .setParallelism(1)

//    // 构建主数据流（关键调整：确保ProcessFunction是Scala版）
    val keyedStream: DataStream[AlgorithResult] = sourceStream  // 显式指定返回类型为Scala版DataStream
      .map { str =>                                             // 转换：将字符串分割为(key, value)
        val parts = str.split("_")                              // 按下划线分割
        val key = if (parts.length > 1) parts.last else str    // 取最后一段作为key（无分割时用原字符串）
        (key, str)
      }.returns(createTypeInformation[(String, String)])       // 显式指定返回类型（解决类型推断问题）
      .keyBy(_._1)                                             // 按key分组（基于第一个元素）
      // 关键调整2：确保AlgorithmTransformate继承Scala版ProcessFunction
      // 应用自定义处理函数：传入侧输出流标签、时间戳、站点配置
      .process(new AlgorithmTransformate(validOutputTag, timestamp, sitePartitionMap))
      .setParallelism(8)                                       // 并行度设为8

//    // 构建复杂流（保持不变）：源数据作为复杂流
    val complexStream: DataStream[AlgorithResult] = sourceStream  // 显式指定类型
      .map { str =>                                               // 转换：按下划线分割，取第一段作为key
        val parts = str.split("_")
        val key = if (parts.length > 1) parts.head else str      //提取第一段作为 key
        (key, str)
      }.returns(createTypeInformation[(String, String)])         // 显式指定返回类型（解决类型推断问题）
      .keyBy(_._1)                                              // 按key分组（基于第一个元素）
      // 应用复杂逻辑处理函数：传入时间戳和站点配置（无侧输出流）
      .process(new AlgorithmTransformateComplex(timestamp, sitePartitionMap))
      .setParallelism(2)                                        // 并行度设为2
//    complexStream.print()

    // 侧输出流与主数据流处理
    // 关键调整3：getSideOutput返回的流会自动转换为Scala版（因OutputTag和ProcessFunction均为Scala类型）
    // 提取侧输出流：从主数据流中获取"valid-points"标签的侧流
    val validStream: DataStream[DiagnosisResult] = keyedStream.getSideOutput(validOutputTag)

    // 处理valid数据（保持不变）
    validStream
      .keyBy((data: DiagnosisResult) => data.expressions.dropRight(6))     // 截取expressions（去掉最后6个字符）作为分组key
      .process(new BatchProcessFunction)                                   // 批处理逻辑
      .setParallelism(6)                                                   // 并行度6

    // 处理主数据流（保持不变）：按station和expressions前缀分组，写入IoTDB
    keyedStream
      .keyBy((data: AlgorithResult) => data.station + "_" + data.expressions.substring(0, 2))    // 组合station和expressions前缀作为key
      .process(new IoTDBNumerSink)         // 写入IoTDB的sink
      .setParallelism(4)                   // 并行度4

//    //合并流并输出（保持不变）：合并主数据流和复杂数据流，输出到Mqtt
    val mergedStream = keyedStream.union(complexStream)
    mergedStream.addSink(new IoTDBtoMqtt).setParallelism(1)     // 写入Mqtt的sink，并行度1
    complexStream.addSink(new IoTDBtoMqtt).setParallelism(1)

    // 启动Flink作业，名称为"Flink State Manager Test"
    env.execute("Flink State Manager Test")
  }
}