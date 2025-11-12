package com.keystar.flink.kr_cache_demo

import com.keystar.flink.kr_cache_demo.JsonProcessingJob.{extractFieldsFromData, extractFieldsFromData02, extractTask1Data, extractTask2Data, getzcPointList}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.util.Collector
import play.api.libs.json.{JsArray, JsValue, Json}

import scala.io.Source

object Test03 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    // 设置 RocksDB 状态后端
    val rocksDbBackend: StateBackend = new RocksDBStateBackend("file:///path/to/checkpoint", true)

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
    val result02: Map[String, List[String]] = extractFieldsFromData02(json02)

    val statjs: Map[String, String] = getzcPointList(json)
    val statjs02: Map[String, String] = getzcPointList(json02)

    // 提取任务1的 I 和 U 字段
    val task1Data: Map[String, Map[String, (String, String)]] = extractTask1Data(json)

    // 提取任务2的 power 字段
    val task2Data: Map[String, Map[String, String]] = extractTask2Data(json02)

    //提取任务2对应的状态字段
    val tjson2data: Map[String, Map[String, (String, String)]] = extractTask3Data(json02)
    // 合并两个结果的字段
    val allFields = (result.keySet ++ result02.keySet).foldLeft(Map[String, List[String]]()) {
      (acc, key) =>
        val listFromResult = result.getOrElse(key, List()).distinct // 假设这里已经去重
        val listFromResult02 = result02.getOrElse(key, List())

        // 对 listFromResult02 进行去重，去除那些在 listFromResult 中已存在的元素
        val filteredListFromResult02 = listFromResult02.filterNot(listFromResult.contains)

        // 合并两个列表，保持顺序
        val combinedList = listFromResult ++ filteredListFromResult02

        acc + (key -> combinedList)
    }

    val javaCollection = new java.util.ArrayList[(String, List[String])]()
    allFields.toList.foreach { element =>
      javaCollection.add(element)
    }

    val allFieldsDataSet: DataSet[(String, List[String])] = env.fromCollection(javaCollection).setParallelism(10)


    // 使用显式类型声明来解决编译异常
    val allFieldsDataSet02: DataSet[(String, List[String])] = env.fromCollection(javaCollection)
      .flatMap(new FlatMapFunction[(String, List[String]), (String, List[String])] {
        override def flatMap(value: (String, List[String]), out: Collector[(String, List[String])]): Unit = {
          val (stationId, fields) = value
          if (fields.size > 2500) {
            // 如果字段数量超过1000，则将其分成多个批次
            fields.grouped(2500).foreach(batch => out.collect((stationId, batch.toList)))
          } else {
            // 否则，直接返回原始数据
            out.collect((stationId, fields))
          }
        }
      })
//      .setParallelism(1) // 根据实际情况调整并行度

    allFieldsDataSet02.rebalance().map(new ProcessStationDataRichMap(statjs, statjs02,task2Data,tjson2data)).collect()
  }

  // 提取任务2的 power 字段
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
}
