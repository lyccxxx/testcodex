package com.keystar.flink.iotdbstream
import com.keystar.flink.iotdbfunction.IotdbFunction.createDiagnosisRuleFromResultSet


import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.configuration.Configuration
import play.api.libs.json.Json

import java.io.File
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.collection.mutable.ListBuffer




class DiagnosisRuleSource(sitePartitionMap: mutable.Map[String, Int]) extends RichParallelSourceFunction[String] {

  // JDBC 连接相关
  private var connection: Connection = _
  private var psFull: PreparedStatement = _
  private var psIncremental: PreparedStatement = _
  private var siteIds:List[String]= _

  // 状态用于保存上次查询时间
  private var firstTime: Timestamp = _
  private var historicalData: ListBuffer[(String,DiagnosisRule)] = ListBuffer.empty[(String,DiagnosisRule)] // 保存历史数据

  private val siteCounters: mutable.Map[String, Long] = mutable.Map.empty

  private var lastQueryTime: LocalDateTime = LocalDateTime.now() // 记录上次查询的时间

  // 查询间隔（例如每天）
  private val queryIntervalMillis = 24 * 60 * 60 * 1000L // 一天

  // 标志位
  @volatile private var isRunning = true


  override def open(parameters: Configuration): Unit = {
    // 初始化数据库连接
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"

    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)
    connection.setAutoCommit(true)


    // 获取首次最大时间
    firstTime = getMaxTime()

    var increSql =""
    // 准备 SQL
    val fullQuery =
      s"""
        |SELECT *,
        |  CASE
        |    WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2
        |    THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
        |    ELSE split_part(iot_fld, '_', 1)
        |  END AS iot_dev
        |FROM public.kr_diagnosis_rules
        |${increSql}
        |ORDER BY updated_at, id
        |""".stripMargin

    psFull = connection.prepareStatement(fullQuery)
    psFull.setFetchSize(1000)


    increSql=" WHERE updated_at > ?"
    val incrementalQuery = fullQuery
    psIncremental = connection.prepareStatement(incrementalQuery)
    psIncremental.setFetchSize(1000)
    siteIds =generatePartitionKeys(sitePartitionMap)
  }

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    // 第一次运行，执行全量拉取
//    fetchAndEmitFullData()
    while (isRunning) {
      siteIds.distinct.foreach { siteId =>
        sourceContext.collect(siteId)
      }
      Thread.sleep(60000)
    }
  }

  private def fetchAndEmitFullData(): Unit = {
    val rs = psFull.executeQuery()
    while (rs.next()) {
      val rule = createDiagnosisRuleFromResultSet(rs)
      val siteId = rule.site_id
      val numPartitions = sitePartitionMap.getOrElse(siteId, 1)
      // 获取或初始化站点专属计数器
      val counter = siteCounters.getOrElse(siteId, 0L)
      siteCounters(siteId) = counter + 1 // 先使用旧值，再递增
      val modValue = counter % numPartitions // 用旧值取模（确保第一个数据从0开始
      val partitionKey = s"${siteId}_${modValue}"
      historicalData.append((partitionKey,rule))
    }
    rs.close()
  }

  override def cancel(): Unit = isRunning = false

  override def close(): Unit = {
    if (psFull != null) psFull.close()
    if (psIncremental != null) psIncremental.close()
    if (connection != null) connection.close()
  }

  private def fetchData(statement: PreparedStatement): ListBuffer[(String,DiagnosisRule)] = {
    var resultSet: ResultSet = null
    try {
      resultSet = statement.executeQuery()
      val rules = ListBuffer[(String,DiagnosisRule)]()
      while (resultSet.next()) {
        val rule = createDiagnosisRuleFromResultSet(resultSet)
        val siteId = rule.site_id
        val counter = siteCounters.getOrElse(siteId, 0L)
        val numPartitions = sitePartitionMap.getOrElse(siteId, 1)
        siteCounters(siteId) = counter + 1 // 先使用旧值，再递增
        val modValue = counter % numPartitions // 用旧值取模（确保第一个数据从0开始
        val partitionKey = s"${siteId}_${modValue}"
        rules.append((partitionKey,rule))
      }
      rules
    } finally {
      if (resultSet != null) resultSet.close()
    }
  }



  // 获取最大时间
  private def getMaxTime(): Timestamp = {
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      val sql = "SELECT max(updated_at) as upat FROM public.kr_diagnosis_rules"
      stmt = connection.prepareStatement(sql)
      rs = stmt.executeQuery()
      var num = new Timestamp(0)
      if (rs.next()) {
        num = rs.getTimestamp("upat")
      }
      num
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
    }
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
}

case class DiagnosisRule(
                          id: Long,
                          site_id: String,
                          iot_tbl: String,
                          iot_dev: String,
                          iot_fld: String,
                          storage_type: String,
                          src_disconn: Boolean,
                          samp_freq_mismatch: Boolean,
                          samp_freq_diag_time: Long,
                          samp_freq_clr_time: Long,
                          conv_amp_factor_sign: Boolean,
                          conv_amp_factor: Double,
                          norm_val_sign: Boolean,
                          norm_val: Double,
                          auto_mon_max_val: Boolean,
                          auto_clr_max_val: Boolean,
                          max_val_thres: Double,
                          max_val_estab_time: Long,
                          max_val_clr_time: Long,
                          auto_mon_min_val: Boolean,
                          auto_clr_min_val: Boolean,
                          min_val_thres: Double,
                          min_val_estab_time: Long,
                          min_val_clr_time: Long,
                          auto_mon_rate_chg: Boolean,
                          auto_clr_rate_chg: Boolean,
                          rate_chg_thres: Double,
                          rate_chg_estab_time: Long,
                          rate_chg_clr_time: Long,
                          auto_mon_dead_zone: Boolean,
                          auto_clr_dead_zone: Boolean,
                          dead_zone_thres_z1: Double,
                          dead_zone_thres_v1: Double,
                          dead_zone_thres_v2: Double,
                          dead_zone_thres_v3: Double,
                          created_at: Timestamp,
                          updated_at: Timestamp
                        )