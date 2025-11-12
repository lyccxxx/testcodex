package com.keystar.flink.diagnos_table

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object FileWrite {
  def main(args: Array[String]): Unit = {
    // 定义文件路径和名称
    val filePath = "202412021601.csv"
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://192.168.5.12:5432/postgres"
    val user = "postgres"
    val password = "123456"

    try {
      // 加载驱动
      Class.forName(driver)

      // 创建数据库连接
      val connection: Connection = DriverManager.getConnection(url, user, password)
      connection.setAutoCommit(false) // 并不是所有数据库都适用，比如 Hive 就不支持，Oracle 不需要

      // 初始查询
      val initialQuery =
        s"""
           |SELECT
           |  id,
           |  site_id,
           |  iot_tbl,
           |  CASE
           |    WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2 THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
           |    ELSE split_part(iot_fld, '_', 1)
           |  END AS iot_dev,
           |  iot_fld,
           |  src_disconn,
           |  storage_type,
           |  samp_freq_mismatch,
           |  samp_freq_diag_time,
           |  samp_freq_clr_time,
           |  conv_amp_factor_sign,
           |  conv_amp_factor,
           |  norm_val_sign,
           |  norm_val,
           |  auto_mon_max_val,
           |  auto_clr_max_val,
           |  max_val_thres,
           |  max_val_estab_time,
           |  max_val_clr_time,
           |  auto_mon_min_val,
           |  auto_clr_min_val,
           |  min_val_thres,
           |  min_val_estab_time,
           |  min_val_clr_time,
           |  auto_mon_rate_chg,
           |  auto_clr_rate_chg,
           |  rate_chg_thres,
           |  rate_chg_estab_time,
           |  rate_chg_clr_time,
           |  auto_mon_dead_zone,
           |  auto_clr_dead_zone,
           |  dead_zone_thres_z1,
           |  dead_zone_thres_v1,
           |  dead_zone_thres_v2,
           |  dead_zone_thres_v3,
           |  created_at,
           |  updated_at
           |FROM public.kr_diagnosis_rules
           |ORDER BY site_id
           |""".stripMargin

      // 准备语句
      val psInitial: PreparedStatement = connection.prepareStatement(initialQuery)
      psInitial.setFetchSize(1000)

      // 执行查询
      val resultSet: ResultSet = psInitial.executeQuery()

      // 创建文件并写入内容
      val file = new File(filePath)
      val writer = new BufferedWriter(new FileWriter(file))

      // 写入 CSV 头
      val columnNames = List(
        "id", "site_id", "iot_tbl", "iot_dev", "iot_fld", "src_disconn", "storage_type",
        "samp_freq_mismatch", "samp_freq_diag_time", "samp_freq_clr_time", "conv_amp_factor_sign",
        "conv_amp_factor", "norm_val_sign", "norm_val", "auto_mon_max_val", "auto_clr_max_val",
        "max_val_thres", "max_val_estab_time", "max_val_clr_time", "auto_mon_min_val",
        "auto_clr_min_val", "min_val_thres", "min_val_estab_time", "min_val_clr_time",
        "auto_mon_rate_chg", "auto_clr_rate_chg", "rate_chg_thres", "rate_chg_estab_time",
        "rate_chg_clr_time", "auto_mon_dead_zone", "auto_clr_dead_zone", "dead_zone_thres_z1",
        "dead_zone_thres_v1", "dead_zone_thres_v2", "dead_zone_thres_v3", "created_at", "updated_at"
      )
      writer.write(columnNames.mkString(",") + "\n")

      // 写入数据
      while (resultSet.next()) {
        val row = List(
          resultSet.getString("id"),
          resultSet.getString("site_id"),
          resultSet.getString("iot_tbl"),
          resultSet.getString("iot_dev"),
          resultSet.getString("iot_fld"),
          resultSet.getString("src_disconn"),
          resultSet.getString("storage_type"),
          resultSet.getString("samp_freq_mismatch"),
          resultSet.getString("samp_freq_diag_time"),
          resultSet.getString("samp_freq_clr_time"),
          resultSet.getString("conv_amp_factor_sign"),
          resultSet.getString("conv_amp_factor"),
          resultSet.getString("norm_val_sign"),
          resultSet.getString("norm_val"),
          resultSet.getString("auto_mon_max_val"),
          resultSet.getString("auto_clr_max_val"),
          resultSet.getString("max_val_thres"),
          resultSet.getString("max_val_estab_time"),
          resultSet.getString("max_val_clr_time"),
          resultSet.getString("auto_mon_min_val"),
          resultSet.getString("auto_clr_min_val"),
          resultSet.getString("min_val_thres"),
          resultSet.getString("min_val_estab_time"),
          resultSet.getString("min_val_clr_time"),
          resultSet.getString("auto_mon_rate_chg"),
          resultSet.getString("auto_clr_rate_chg"),
          resultSet.getString("rate_chg_thres"),
          resultSet.getString("rate_chg_estab_time"),
          resultSet.getString("rate_chg_clr_time"),
          resultSet.getString("auto_mon_dead_zone"),
          resultSet.getString("auto_clr_dead_zone"),
          resultSet.getString("dead_zone_thres_z1"),
          resultSet.getString("dead_zone_thres_v1"),
          resultSet.getString("dead_zone_thres_v2"),
          resultSet.getString("dead_zone_thres_v3"),
          resultSet.getString("created_at"),
          resultSet.getString("updated_at")
        ).mkString(",")
        writer.write(row + "\n")
      }

      // 关闭资源
      writer.close()
      resultSet.close()
      psInitial.close()
      connection.commit()
      connection.close()

      println(s"文件已成功写入: $filePath")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        // 在发生异常时回滚事务
//        if (connection != null) {
//          connection.rollback()
//          connection.close()
//        }
    }
  }
}