package com.keystar.flink.kr_cache_demo

import com.github.tototoshi.csv.CSVReader
import com.keystar.flink.iotdbstream.DiagnosisRule
import org.apache.flink.configuration.Configuration

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat

class Test04BatchSource {
  private var connection: Connection = _
  private var psInitial: PreparedStatement = _
  private var psIncremental: PreparedStatement = _
  private var firstTime: Timestamp = _
  private var file: String = _
  private var reader: CSVReader = _
  private var numParallelInstances: Int = 0
  private var parallelInstanceId: Int = 0
  private var start_time: Long = _
  private var assignedSiteIds: List[List[String]] = _

  def open(parameters: Configuration): Unit = {
    start_time = System.currentTimeMillis()
    // 指定驱动
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://192.168.5.12:5432/postgres"
    val user = "postgres"
    val password = "123456"
    Class.forName(driver)
    // 创建数据库连接
    connection = DriverManager.getConnection(url, user, password)

    connection.setAutoCommit(false) // 并不是所有数据库都适用，比如hive就不支持，orcle不需要
    // 获取首次最大时间
    firstTime = getMaxTime(connection)
    // 获取文件路径
    file="E:\\kr_diagnosis_rules_202412091706.csv"  //这个
//    file = "/home/test/kr_diagnosis_rules_202412091706.csv"

    // 打开文件
    reader = CSVReader.open(new java.io.File(file))
    // 跳过标题行
    reader.readNext()
    assignedSiteIds = reader.all()
    reader.close()
    // 准备增量查询语句
    val incrementalQuery =
      s"""
         |SELECT *,CASE
         |        WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2 THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
         |        ELSE split_part(iot_fld, '_', 1)
         |    END AS iot_dev
         |FROM public.kr_diagnosis_rules
         |WHERE updated_at > ?
         |-- and site_id in('1269014857443188597','1269014857442188595')
         |-- and iot_fld in('I008_3_D0026','I004_9_D0053','I004_16_D0050','I013_7_D0053','I082_7_YX0001','I074_8_INVERTER_PhsC_U')
         |order by site_id
         |""".stripMargin
    psIncremental = connection.prepareStatement(incrementalQuery)
    psIncremental.setFetchSize(1000)
  }

  // 获取最大时间
  private def getMaxTime(connection: Connection, tag: Timestamp*): Timestamp = {
    val sql = if (tag.isEmpty) {
      """
        |SELECT max(updated_at) as upat FROM public.kr_diagnosis_rules
        |""".stripMargin
    } else {
      // 提取第一个 Timestamp 参数
      val tagValue = tag.head
      s"SELECT distinct updated_at as upat FROM public.kr_diagnosis_rules WHERE updated_at > '${tagValue}'"
    }
    val stmt = connection.createStatement()
    val rs = stmt.executeQuery(sql)
    var num = new Timestamp(0)
    if (rs.next()) {
      num = rs.getTimestamp("upat")
    }
    rs.close()
    stmt.close()
    num
  }

  def getAllRules(): List[DiagnosisRule] = {
    // 处理 CSV 文件数据
    val csvRules = assignedSiteIds.map(parseLineToDiagnosisRule)

    // 处理增量数据
    psIncremental.setTimestamp(1, firstTime)
    val incrementalResultSet = psIncremental.executeQuery()

    val incrementalRules = scala.collection.mutable.ListBuffer[DiagnosisRule]()
    while (incrementalResultSet.next()) {
      val rule = DiagnosisRule(
        id = incrementalResultSet.getLong("id"),
        site_id = incrementalResultSet.getString("site_id"),
        iot_tbl = incrementalResultSet.getString("iot_tbl"),
        iot_fld = incrementalResultSet.getString("iot_fld"),
        src_disconn = incrementalResultSet.getBoolean("src_disconn"),
        samp_freq_mismatch = incrementalResultSet.getBoolean("samp_freq_mismatch"),
        samp_freq_diag_time = incrementalResultSet.getLong("samp_freq_diag_time"),
        samp_freq_clr_time = incrementalResultSet.getLong("samp_freq_clr_time"),
        conv_amp_factor_sign = incrementalResultSet.getBoolean("conv_amp_factor_sign"),
        conv_amp_factor = incrementalResultSet.getDouble("conv_amp_factor"),
        norm_val_sign = incrementalResultSet.getBoolean("norm_val_sign"),
        norm_val = incrementalResultSet.getDouble("norm_val"),
        auto_mon_max_val = incrementalResultSet.getBoolean("auto_mon_max_val"),
        auto_clr_max_val = incrementalResultSet.getBoolean("auto_clr_max_val"),
        max_val_thres = incrementalResultSet.getDouble("max_val_thres"),
        max_val_estab_time = incrementalResultSet.getLong("max_val_estab_time"),
        max_val_clr_time = incrementalResultSet.getLong("max_val_clr_time"),
        auto_mon_min_val = incrementalResultSet.getBoolean("auto_mon_min_val"),
        auto_clr_min_val = incrementalResultSet.getBoolean("auto_clr_min_val"),
        min_val_thres = incrementalResultSet.getDouble("min_val_thres"),
        min_val_estab_time = incrementalResultSet.getLong("min_val_estab_time"),
        min_val_clr_time = incrementalResultSet.getLong("min_val_clr_time"),
        auto_mon_rate_chg = incrementalResultSet.getBoolean("auto_mon_rate_chg"),
        auto_clr_rate_chg = incrementalResultSet.getBoolean("auto_clr_rate_chg"),
        rate_chg_thres = incrementalResultSet.getDouble("rate_chg_thres"),
        rate_chg_estab_time = incrementalResultSet.getLong("rate_chg_estab_time"),
        rate_chg_clr_time = incrementalResultSet.getLong("rate_chg_clr_time"),
        auto_mon_dead_zone = incrementalResultSet.getBoolean("auto_mon_dead_zone"),
        auto_clr_dead_zone = incrementalResultSet.getBoolean("auto_clr_dead_zone"),
        dead_zone_thres_z1 = incrementalResultSet.getDouble("dead_zone_thres_z1"),
        dead_zone_thres_v1 = incrementalResultSet.getDouble("dead_zone_thres_v1"),
        dead_zone_thres_v2 = incrementalResultSet.getDouble("dead_zone_thres_v2"),
        dead_zone_thres_v3 = incrementalResultSet.getDouble("dead_zone_thres_v3"),
        created_at = incrementalResultSet.getTimestamp("created_at"),
        updated_at = incrementalResultSet.getTimestamp("updated_at"),
        storage_type = incrementalResultSet.getString("storage_type"),
        iot_dev = incrementalResultSet.getString("iot_dev")
      )
      incrementalRules += rule
      if (rule.updated_at.after(firstTime)) {
        firstTime = rule.updated_at
      }
    }
    incrementalResultSet.close()

    csvRules ++ incrementalRules.toList
  }

  def cancel(): Unit = {
    if (psInitial != null) {
      psInitial.close()
    }
    if (psIncremental != null) {
      psIncremental.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  // 解析每一行数据
  def parseLineToDiagnosisRule(parts: List[String]): DiagnosisRule = {
    val id = parts.lift(0).map(_.toLong).getOrElse(0L)
    val site_id = parts.lift(1).getOrElse("")
    val iot_tbl = parts.lift(2).getOrElse("")
    val iot_dev = parts.lift(3).getOrElse("")
    val iot_fld = parts.lift(4).getOrElse("")
    val src_disconn = parts.lift(5).map(_.toBoolean).getOrElse(false)
    val storage_type = parts.lift(6).getOrElse("")
    val samp_freq_mismatch = parts.lift(7).map(_.toBoolean).getOrElse(false)
    val samp_freq_diag_time = parts.lift(8).map(_.toLong).getOrElse(0L)
    val samp_freq_clr_time = parts.lift(9).map(_.toLong).getOrElse(0L)
    val conv_amp_factor_sign = parts.lift(10).map(_.toBoolean).getOrElse(false)
    val conv_amp_factor = parts.lift(11).map(_.toDouble).getOrElse(0.0)
    val norm_val_sign = parts.lift(12).map(_.toBoolean).getOrElse(false)
    val norm_val = parts.lift(13).map(_.toDouble).getOrElse(0.0)
    val auto_mon_max_val = parts.lift(14).map(_.toBoolean).getOrElse(false)
    val auto_clr_max_val = parts.lift(15).map(_.toBoolean).getOrElse(false)
    val max_val_thres = parts.lift(16).map(_.toDouble).getOrElse(0.0)
    val max_val_estab_time = parts.lift(17).map(_.toLong).getOrElse(0L)
    val max_val_clr_time = parts.lift(18).map(_.toLong).getOrElse(0L)
    val auto_mon_min_val = parts.lift(19).map(_.toBoolean).getOrElse(false)
    val auto_clr_min_val = parts.lift(20).map(_.toBoolean).getOrElse(false)
    val min_val_thres = parts.lift(21).map(_.toDouble).getOrElse(0.0)
    val min_val_estab_time = parts.lift(22).map(_.toLong).getOrElse(0L)
    val min_val_clr_time = parts.lift(23).map(_.toLong).getOrElse(0L)
    val auto_mon_rate_chg = parts.lift(24).map(_.toBoolean).getOrElse(false)
    val auto_clr_rate_chg = parts.lift(25).map(_.toBoolean).getOrElse(false)
    val rate_chg_thres = parts.lift(26).map(_.toDouble).getOrElse(0.0)
    val rate_chg_estab_time = parts.lift(27).map(_.toLong).getOrElse(0L)
    val rate_chg_clr_time = parts.lift(28).map(_.toLong).getOrElse(0L)
    val auto_mon_dead_zone = parts.lift(29).map(_.toBoolean).getOrElse(false)
    val auto_clr_dead_zone = parts.lift(30).map(_.toBoolean).getOrElse(false)
    val dead_zone_thres_z1 = parts.lift(31).map(_.toDouble).getOrElse(0.0)
    val dead_zone_thres_v1 = parts.lift(32).map(_.toDouble).getOrElse(0.0)
    val dead_zone_thres_v2 = parts.lift(33).map(_.toDouble).getOrElse(0.0)
    val dead_zone_thres_v3 = parts.lift(34).map(_.toDouble).getOrElse(0.0)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val createdAt = if (parts.lift(35).getOrElse("").length != 0) {
      val date = dateFormat.parse(parts.lift(35).getOrElse(""))
      new Timestamp(date.getTime)
    } else {
      null // or some default value
    }
    val updatedAt = if (parts.lift(36).getOrElse("").length != 0) {
      val date = dateFormat.parse(parts.lift(36).getOrElse(""))
      new Timestamp(date.getTime)
    } else null // or some default value
    DiagnosisRule(
      id = id,
      site_id = site_id,
      iot_tbl = iot_tbl,
      iot_dev = iot_dev,
      iot_fld = iot_fld,
      src_disconn = src_disconn,
      storage_type = storage_type,
      samp_freq_mismatch = samp_freq_mismatch,
      samp_freq_diag_time = samp_freq_diag_time,
      samp_freq_clr_time = samp_freq_clr_time,
      conv_amp_factor_sign = conv_amp_factor_sign,
      conv_amp_factor = conv_amp_factor,
      norm_val_sign = norm_val_sign,
      norm_val = norm_val,
      auto_mon_max_val = auto_mon_max_val,
      auto_clr_max_val = auto_clr_max_val,
      max_val_thres = max_val_thres,
      max_val_estab_time = max_val_estab_time,
      max_val_clr_time = max_val_clr_time,
      auto_mon_min_val = auto_mon_min_val,
      auto_clr_min_val = auto_clr_min_val,
      min_val_thres = min_val_thres,
      min_val_estab_time = min_val_estab_time,
      min_val_clr_time = min_val_clr_time,
      auto_mon_rate_chg = auto_mon_rate_chg,
      auto_clr_rate_chg = auto_clr_rate_chg,
      rate_chg_thres = rate_chg_thres,
      rate_chg_estab_time = rate_chg_estab_time,
      rate_chg_clr_time = rate_chg_clr_time,
      auto_mon_dead_zone = auto_mon_dead_zone,
      auto_clr_dead_zone = auto_clr_dead_zone,
      dead_zone_thres_z1 = dead_zone_thres_z1,
      dead_zone_thres_v1 = dead_zone_thres_v1,
      dead_zone_thres_v2 = dead_zone_thres_v2,
      dead_zone_thres_v3 = dead_zone_thres_v3,
      created_at = createdAt,
      updated_at = updatedAt
    )
  }
}

