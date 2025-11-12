package com.keystar.flink.kr_protocol_stream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import scala.collection.mutable.ListBuffer
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

// 定义 KrProtocolData 类来表示 kr_protocol_data 表的数据
//case class KrProtocolData(
//                           id: Long,
//                           no: Option[Long],
//                           `type`: Option[String],
//                           ip: Option[String],
//                           port: Option[Int],
//                           address: Option[Int],
//                           name: Option[String],
//                           data_type: Option[String],
//                           comment: Option[String],
//                           data_template_id: Option[Long],
//                           data_template_name: Option[String],
//                           unit: Option[String],
//                           description: Option[String],
//                           validation: Option[String],
//                           precision: Option[Float],
//                           data_lower_limit: Option[Float],
//                           data_upper_limit: Option[Double],
//                           collect_interval: Option[Int],
//                           alarm: Option[String],
//                           critical_lower_limit: Option[Float],
//                           warning_lower_limit: Option[Float],
//                           warning_upper_limit: Option[Float],
//                           critical_upper_limit: Option[Float],
//                           channel_id: Option[Long],
//                           stat_period: Option[Int],
//                           alarm_level: Option[String],
//                           data_name: Option[String],
//                           bind_channel: Option[String],
//                           total_data: Option[Int],
//                           yx_data: Option[String],
//                           yc_data: Option[String],
//                           no104: Option[Long],
//                           create_time: Option[Timestamp],
//                           update_time: Option[Timestamp],
//                           site_id: Option[Long],
//                           mqtt_base_id: Option[Long],
//                           database_id: Option[Long],
//                           table_id: Option[Long],
//                           access_mode: Option[String],
//                           key_name: Option[String],
//                           default_bit: Option[Short],
//                           message_name: Option[String],
//                           data_source: Option[String],
//                           detail_source: Option[String],
//                           device_name: Option[String],
//                           device_id: Option[Long],
//                           bind_device: Option[String],
//                           td_id: Option[String],
//                           field_coefficient: Option[Float],
//                           point_name: Option[String],
//                           device_type: Option[String],
//                           iot_table: Option[String],
//                           iot_field: Option[String],
//                           storage_type: Option[String],
//                           site_name: Option[String],
//                           enable: Option[String],
//                           abbreviated_name: Option[String],
//                           classification: Option[String],
//                           auxiliary_name: Option[String],
//                           switch_cabinet: Option[String]
//                         )

case class KrProtocolData(
                           id: Long,
                           data_type: Option[String],
                           unit: Option[String],
                           validation: Option[String],
                           precision: Option[Float],
                           data_lower_limit: Option[Float],
                           data_upper_limit: Option[Double],
                           collect_interval: Option[Int],
                           alarm: Option[String],
                           critical_lower_limit: Option[Float],
                           warning_lower_limit: Option[Float],
                           warning_upper_limit: Option[Float],
                           critical_upper_limit: Option[Float],
                           alarm_level: Option[String],
                           create_time: Option[Timestamp],
                           update_time: Option[Timestamp],
                           site_id: Option[Long],
                           device_id: Option[Long],
                           iot_table: Option[String],
                           iot_field: Option[String],
                           storage_type: Option[String]
                         )



class PostgresqlSource(site:List[String]) extends RichParallelSourceFunction[KrProtocolData] {

  private var connection: Connection = _
  private var psInitial: PreparedStatement = _
  private var psIncremental: PreparedStatement = _
  private var isRunning: Boolean = true
  private var firstTime: Timestamp = _
  private var numParallelInstances: Int = 0
  private var parallelInstanceId: Int = 0
  private var start_time: Long = _
  private var historicalData: ListBuffer[KrProtocolData] = ListBuffer.empty[KrProtocolData] // 保存历史数据
  private var lastQueryTime: LocalDateTime = LocalDateTime.now() // 记录上次查询的时间
  private var isFirstRun: Boolean = true // 标记是否是首次运行
  private val logger = LoggerFactory.getLogger(this.getClass)


  override def open(parameters: Configuration): Unit = {
    start_time = System.currentTimeMillis()
    numParallelInstances = getRuntimeContext.getNumberOfParallelSubtasks
    parallelInstanceId = getRuntimeContext.getIndexOfThisSubtask

    // 指定驱动
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url02 = "jdbc:postgresql://192.168.5.12:5432/data"
    val url = "jdbc:postgresql://10.190.6.97:5432/data"  //高井
    val url03 = "jdbc:postgresql://10.122.1.72:5432/data"
    val url01 = "jdbc:postgresql://172.16.1.34:5432/data" //木垒
    val user = "postgres"
    val password02 = "123456"
    val password = "K0yS@2024"
    Class.forName(driver)

    // 创建数据库连接
    connection = DriverManager.getConnection(url, user, password)
    connection.setAutoCommit(false)

    // 获取首次最大时间
    firstTime = getMaxTime()

    // 准备初始查询语句
    val initialQuery =
      s"""
         |SELECT id,
         |data_type,
         |unit,
         |validation,
         |precision,
         |data_lower_limit,
         |data_upper_limit,
         |collect_interval,
         |alarm,
         |critical_lower_limit,
         |warning_lower_limit,
         |warning_upper_limit,
         |critical_upper_limit,
         |alarm_level,
         |create_time,
         |update_time,
         |site_id,
         |device_id,
         |iot_table,
         |iot_field,
         |storage_type
         |FROM public.kr_protocol_data
         |-- where validation ='Y'
         |-- and iot_field ='I015_1_INVERTER_Line01_U' and site_id =1816341324371066880
         |-- and site_id  in(1821385107949223936)
         |where  site_id!='-1' and validation ='Y'
         |and iot_table in ('${site.map(_.replace("'", "''")).mkString("', '")}')
         |-- and iot_field in('I006_16_INVERTER_Line16_I','I006_14_INVERTER_Room_Tmp')
         |""".stripMargin
    psInitial = connection.prepareStatement(initialQuery)
    psInitial.setFetchSize(1000)






    // 准备增量查询语句
    val incrementalQuery =
      s"""
         |SELECT id,
         |data_type,
         |unit,
         |validation,
         |precision,
         |data_lower_limit,
         |data_upper_limit,
         |collect_interval,
         |alarm,
         |critical_lower_limit,
         |warning_lower_limit,
         |warning_upper_limit,
         |critical_upper_limit,
         |alarm_level,
         |create_time,
         |update_time,
         |site_id,
         |device_id,
         |iot_table,
         |iot_field,
         |storage_type
         |FROM public.kr_protocol_data
         |WHERE validation ='Y' and update_time > ?
         |order by iot_field
         |""".stripMargin
    psIncremental = connection.prepareStatement(incrementalQuery)
    psIncremental.setFetchSize(1000)
  }

  // 获取最大时间
  private def getMaxTime(): Timestamp = {
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      val sql = "SELECT max(update_time) as upat FROM public.kr_protocol_data"
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

  override def run(sourceContext: SourceContext[KrProtocolData]): Unit = {
    // 读取初始数据
    historicalData ++= fetchData(psInitial)
    var num =0
    while (isRunning) {
      num+=1
      logger.info(s"每次往下游发送数据：${num}")
        val currentTime = LocalDateTime.now()
        // 计算距离上次查询是否已经过了一天
        if (ChronoUnit.DAYS.between(lastQueryTime, currentTime) >= 1) {
          // 检查增量数据
          psIncremental.setTimestamp(1, firstTime)
          val incrementalData = fetchData(psIncremental)

          if (incrementalData.nonEmpty) {
            // 合并增量数据到历史数据
            historicalData = historicalData.filterNot(oldData => incrementalData.exists(newData => newData.id == oldData.id)) ++ incrementalData
            incrementalData.foreach { rule =>
              rule.update_time.foreach { updateTime =>
                if (updateTime.after(firstTime)) {
                  firstTime = updateTime
                }
              }
            }
          }
          lastQueryTime = currentTime // 更新上次查询时间
        }
      // 对历史数据按iot_field字段排序
      val totalData = historicalData.sortBy(_.iot_field)
      // 计算拆分点，将数据分为两半
      val halfIndex = totalData.length / 2
      val firstHalf = totalData.take(halfIndex)
      val secondHalf = totalData.drop(halfIndex)

      // 第一次发送前半部分数据
      firstHalf.grouped(1000).foreach(batch => batch.foreach(sourceContext.collect))
      // 第二次发送后半部分数据
      secondHalf.grouped(1000).foreach(batch => batch.foreach(sourceContext.collect))
        logger.info(s"发送数据结束的数据：${num}")
      }
  }
  private def fetchData(statement: PreparedStatement): ListBuffer[KrProtocolData] = {
    var resultSet: ResultSet = null
    try {
      resultSet = statement.executeQuery()
      val rules = ListBuffer[KrProtocolData]()
      while (resultSet.next()) {
        val rule = KrProtocolData(
          id = resultSet.getLong("id"),
          data_type = Option(resultSet.getString("data_type")),
          unit = Option(resultSet.getString("unit")),
          validation = Option(resultSet.getString("validation")),
          precision = Option(resultSet.getFloat("precision")).filter(_ != 0),
          data_lower_limit = Option(resultSet.getFloat("data_lower_limit")).filter(_ != 0),
          data_upper_limit = Option(resultSet.getDouble("data_upper_limit")).filter(_ != 0),
          collect_interval = Option(resultSet.getInt("collect_interval")).filter(_ != 0),
          alarm = Option(resultSet.getString("alarm")),
          critical_lower_limit = Option(resultSet.getFloat("critical_lower_limit")).filter(_ != 0),
          warning_lower_limit = Option(resultSet.getFloat("warning_lower_limit")).filter(_ != 0),
          warning_upper_limit = Option(resultSet.getFloat("warning_upper_limit")).filter(_ != 0),
          critical_upper_limit = Option(resultSet.getFloat("critical_upper_limit")).filter(_ != 0),
          alarm_level = Option(resultSet.getString("alarm_level")),
          create_time = Option(resultSet.getTimestamp("create_time")),
          update_time = Option(resultSet.getTimestamp("update_time")),
          site_id = Option(resultSet.getLong("site_id")).filter(_ != 0),
          device_id = Option(resultSet.getLong("device_id")),
          iot_table = Option(resultSet.getString("iot_table")),
          iot_field = Option(resultSet.getString("iot_field")),
          storage_type = Option(resultSet.getString("storage_type"))
        )
        rules += rule
      }
      return rules
    } finally {
      if (resultSet != null) resultSet.close()
    }
  }

  override def cancel(): Unit = {
    isRunning = false
    try {
      if (psInitial != null) psInitial.close()
      if (psIncremental != null) psIncremental.close()
      if (connection != null) connection.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}