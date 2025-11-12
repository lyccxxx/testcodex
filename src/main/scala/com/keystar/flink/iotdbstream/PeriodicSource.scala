package com.keystar.flink.iotdbstream

import com.keystar.flink.iotdbfunction.IotdbFunction.{get_Connect, get_Pgcount, get_SiteIds}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.sql.Connection
import scala.collection.mutable.ListBuffer

class PeriodicSource(id:Int) extends RichParallelSourceFunction[(String, String)] {
  var isRunning = true
  private var siteIds:ListBuffer[(String, String)]= _
  private var numParallelInstances:Int= _
  private var parallelInstanceId:Int=_
  private var assignedSiteIds: ListBuffer[(String, String)]=_
  override def open(parameters: Configuration): Unit ={
    super.open(parameters)
    val connection = get_Connect()
    siteIds = get_SiteIds(connection,id)
    numParallelInstances = getRuntimeContext.getNumberOfParallelSubtasks
    parallelInstanceId = getRuntimeContext.getIndexOfThisSubtask
    connection.close()
    //     根据并行度分配 siteIds
    assignedSiteIds = siteIds.zipWithIndex.filter { case (_, index) =>
      index % numParallelInstances == parallelInstanceId
    }.map(_._1)
  }
  override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
    while (isRunning) {

      assignedSiteIds.foreach { siteId =>
        ctx.collect(siteId)
      }
      //在这一块发送请求，才能保证数据的流向

      Thread.sleep(180000) // 每分钟生成一次数据
    }
  }
  override def cancel(): Unit = {
    isRunning = false
  }
}

