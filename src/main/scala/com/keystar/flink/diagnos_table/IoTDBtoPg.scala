package com.keystar.flink.diagnos_table

import com.github.tototoshi.csv._

import java.sql.{Connection, DriverManager, Statement}
import java.sql.{Connection, DriverManager, PreparedStatement}

object IoTDBtoPg {
  def main(args: Array[String]): Unit = {
    val pgUrl = "jdbc:postgresql://172.16.1.34:5432/data?serverTimezone=Asia/Shanghai&useSSL=false&allowMultiQueries=true&currentSchema=public"
    val driver = "org.postgresql.Driver"
    val pgUser = "postgres"
    val pgPassword = "K0yS@2024"
    val file = "D:\\tblSensorHardErrByEachDianWei_FDC03.csv"

    // 加载驱动
    Class.forName(driver)

    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      // 建立数据库连接
      conn = DriverManager.getConnection(pgUrl, pgUser, pgPassword)
      conn.setAutoCommit(false)

      // 准备 SQL 语句
      val sql =
        """
          |INSERT INTO public.kr_diagnosis_rules
          |  (id, site_id, iot_tbl, iot_fld, src_disconn, samp_freq_mismatch, samp_freq_diag_time, samp_freq_clr_time,
          |   conv_amp_factor_sign, conv_amp_factor, norm_val_sign, norm_val, auto_mon_max_val, auto_clr_max_val, max_val_thres,
          |   max_val_estab_time, max_val_clr_time, auto_mon_min_val, auto_clr_min_val, min_val_thres, min_val_estab_time,
          |   min_val_clr_time, auto_mon_rate_chg, auto_clr_rate_chg, rate_chg_thres, rate_chg_estab_time, rate_chg_clr_time,
          |   auto_mon_dead_zone, auto_clr_dead_zone, dead_zone_thres_z1, dead_zone_thres_v1, dead_zone_thres_v2, dead_zone_thres_v3,
          |   storage_type)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.stripMargin

      pstmt = conn.prepareStatement(sql)

      // 使用 com.github.tototoshi.csv 读取 CSV 文件
      val reader = CSVReader.open(new java.io.File(file))
      val headers = reader.readNext()
//      println(headers.getOrElse(Array.empty).mkString(", "))

      var lineCount = 0
      var id = 0

      // 处理每一行数据
      reader.foreach { row =>
        lineCount += 1

        try {
          id += 1
          val timeseries = row.headOption.getOrElse("")
          val datatype = if (row.size > 3) row(3) else ""
          val part = timeseries.split("\\.")
          val iot_tbl = part.dropRight(1).mkString(".")
          val site_id = part.drop(2).mkString(".").split("\\.").headOption.getOrElse("")
          val iot_fld = part.lastOption.getOrElse("")

          // 设置参数
          pstmt.setInt(1, id)
          pstmt.setString(2, site_id)
          pstmt.setString(3, iot_tbl)
          pstmt.setString(4, iot_fld)
          pstmt.setBoolean(5, true)
          pstmt.setBoolean(6, true)
          pstmt.setInt(7, 0)
          pstmt.setInt(8, 60)
          pstmt.setBoolean(9, true)
          pstmt.setInt(10, 1)
          pstmt.setBoolean(11, true)
          pstmt.setInt(12, 100)
          pstmt.setBoolean(13, true)
          pstmt.setBoolean(14, true)
          pstmt.setInt(15, 250)
          pstmt.setInt(16, 60)
          pstmt.setInt(17, 60)
          pstmt.setBoolean(18, true)
          pstmt.setBoolean(19, true)
          pstmt.setInt(20, 120)
          pstmt.setInt(21, 60)
          pstmt.setInt(22, 60)
          pstmt.setBoolean(23, true)
          pstmt.setBoolean(24, true)
          pstmt.setInt(25, 50)
          pstmt.setInt(26, 60)
          pstmt.setInt(27, 60)
          pstmt.setBoolean(28, true)
          pstmt.setBoolean(29, true)
          pstmt.setInt(30, 120)
          pstmt.setInt(31, 10)
          pstmt.setInt(32, 15)
          pstmt.setInt(33, 20)
          pstmt.setString(34, datatype)

          // 执行插入
          pstmt.executeUpdate()

          if (lineCount % 1000 == 0) {
            println(s"已处理 $lineCount 行")
          }
        } catch {
          case e: Exception =>
            println(s"处理第 $lineCount 行时出错: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      // 提交事务
      conn.commit()
      println(s"成功导入 $lineCount 行数据")

    } catch {
      case e: Exception =>
        println(s"发生错误: ${e.getMessage}")
        e.printStackTrace()
        if (conn != null) {
          try {
            conn.rollback()
          } catch {
            case rollbackEx: Exception =>
              println(s"回滚事务失败: ${rollbackEx.getMessage}")
          }
        }
    } finally {
      // 关闭资源
      if (pstmt != null) try pstmt.close() catch { case _: Throwable => }
      if (conn != null) try conn.close() catch { case _: Throwable => }
    }
  }
}
