package com.keystar.flink.kr_cache_demo

import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import com.keystar.flink.kr_cache_demo.FlinkBatchDemo.getSqlQuery
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}

class DiagnosisRuleReducer extends GroupReduceFunction[DiagnosisRule, IoTDBReading] {
  override def reduce(iterable: lang.Iterable[DiagnosisRule], collector: Collector[IoTDBReading]): Unit = {
    val rules = iterable.asScala.toList
    val batchSize = 300 // 每个批次的大小
    val batches = rules.grouped(batchSize).toList

    batches.foreach { batch =>
      val iotFlds = batch.map(_.iot_fld).mkString(",")
      val device = batch.head.iot_tbl
      val lastTsOpt = None // 这里简单假设没有上次时间戳，你可以根据实际情况修改
      val query = getSqlQuery(lastTsOpt, iotFlds, device)
      val response = sendRequest(query)
      val data =handleResponse(response)
      val readings = handleResponse(response)
      println(s"Number of IoTDBReading records: ${readings.size}")
//      data.foreach(collector.collect) // 输出每个 IoTDBReading 对象
    }
  }
}