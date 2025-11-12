package com.keystar.flink.kr_json


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SitedataSource(sitepartion:mutable.Map[String,Int]) extends RichParallelSourceFunction[String] {
  var isRunning = true
  private var siteIds:List[String]= _
  private var numParallelInstances:Int= _
  private var parallelInstanceId:Int=_
  private var assignedSiteIds: List[String]=_
  override def open(parameters: Configuration): Unit ={
    super.open(parameters)
//    val connection = get_Connect() "root.ln.`1521714652626283467`","root.ln.`1523723612516384521`"
    siteIds =generatePartitionKeys(sitepartion) // get_SiteIds(connection)
    numParallelInstances = getRuntimeContext.getNumberOfParallelSubtasks
    parallelInstanceId = getRuntimeContext.getIndexOfThisSubtask
//    connection.close()
    //     根据并行度分配 siteIds
    assignedSiteIds = siteIds.distinct
//      .zipWithIndex.filter { case (_, index) =>
//      index % numParallelInstances == parallelInstanceId
//    }.map(_._1)
  }
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    var firstRun =false
    while (isRunning) {

      assignedSiteIds.foreach { siteId =>
        ctx.collect(siteId)
      }
      if(!firstRun){
        //在这一块发送请求，才能保证数据的流向
        Thread.sleep(90000) // 每分钟生成一次数据
      }
    }
  }
  override def cancel(): Unit = {
    isRunning = false
  }

  def generatePartitionKeys01(sitePartitionMap: mutable.Map[String, Int]): List[String] = {
    sitePartitionMap.flatMap { case (siteId, numPartitions) =>
      (0 until numPartitions).map(modValue => s"${siteId}_${modValue}")
    }.toList
  }

  def generatePartitionKeys(sitePartitionMap: mutable.Map[String, Int]): List[String] = {
    var offset = 0  // 记录当前累计的分区数量

    sitePartitionMap.toList.flatMap { case (siteId, numPartitions) =>
      val keys = (0 until numPartitions).map { i =>
        s"${siteId}_${offset + i}"
      }.toList
      offset += numPartitions  // 更新累计偏移量
      keys
    }
  }

  def get_SiteIds(connect:Connection): ListBuffer[String] = {
    val sql =
      s"""
         |with a as(
         |SELECT
         |  case when site_id in('1821385107949223936','1801181094087753728','1816341324371066880','1821385428708622336','1269014857442188595')
         |  then concat(iot_table,'.',substr(device_name,1,4)) else iot_table end as  iot_table2
         |FROM
         |  public.kr_protocol_data
         |  where iot_table !='root.ln.`null`'
         | GROUP BY
         |  iot_table2
         |)
         |select * from a
         |""".stripMargin
    val stmt = connect.createStatement()
    // 执行查询
    val rs: ResultSet = stmt.executeQuery(sql)
    // 创建空的可变列表
    val emptyBuffer: ListBuffer[String] = ListBuffer.empty[String]
    // 遍历结果集并添加数据
    while (rs.next()) {
      emptyBuffer += (rs.getString("iot_table2"))
    }
    // 关闭资源
    rs.close()
    stmt.close()
    emptyBuffer
  }
  def get_Connect(): Connection = {
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"

    val url1 = "jdbc:postgresql://192.168.5.12:5432/data"
    val user1 = "postgres"
    val password1 = "123456"


    Class.forName(driver)
    // 创建数据库连接
    val connection = DriverManager.getConnection(url, user, password)
    connection
  }
}

