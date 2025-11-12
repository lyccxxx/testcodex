package com.keystar.flink.diagnos_table

import scala.collection.mutable.ArrayBuffer

object IoTDBCount {
  def main(args: Array[String]): Unit = {
    import java.sql.{Connection, DriverManager, ResultSet, Statement}

    val pgUrl = "jdbc:postgresql://192.168.5.12:5432/postgres?serverTimezone=Asia/Shanghai&useSSL=false&allowMultiQueries=true&currentSchema=public"
    val pgUser = "postgres"
    val pgPassword = "123456"

    // 连接到PostgreSQL
    Class.forName("org.postgresql.Driver")
    val pgConn: Connection = DriverManager.getConnection(pgUrl, pgUser, pgPassword)
    val pgStmt: Statement = pgConn.createStatement()

    val iotdbUrl = "jdbc:iotdb://192.168.5.13:6667"
    val iotdbUser = "root"
    val iotdbPassword = "root"

    // 连接到IoTDB
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver")
    val iotdbConn: Connection = DriverManager.getConnection(iotdbUrl, iotdbUser, iotdbPassword)
    val iotdbStmt: Statement = iotdbConn.createStatement()

    val query = """
                  |select iot_tbl ,iot_fld
                  |from public.kr_diagnosis_rules
                  |where iot_tbl IN ('root.ln.`1821385107949223936`','root.ln.`1269014857442188595`','root.ln.`1821385428708622336`')
                  |AND iot_fld   LIKE 'S001_%'
                  |group by iot_tbl ,iot_fld
                  |""".stripMargin

    val rs: ResultSet = pgStmt.executeQuery(query)
    var num = 0
    var num1= 0
    var num2= 0

    val time = 1725076515000L
    var buffer: ArrayBuffer[String]   = ArrayBuffer()
    var buffer1: ArrayBuffer[String]  = ArrayBuffer()
    var buffer2: ArrayBuffer[String]  = ArrayBuffer()
    var buffer3: ArrayBuffer[String]  = ArrayBuffer()
    def timeseriesExists(checkSql: String): Boolean = {
      var exists = false
      var checkRs: ResultSet = null
      try {
        checkRs = iotdbStmt.executeQuery(checkSql)
        exists = checkRs.next()
      } catch {
        case e: Exception =>
          println(s"Error executing query $checkSql: ${e.getMessage}")
      } finally {
        if (checkRs != null) checkRs.close()
      }
      exists
    }

    try {
      while (rs.next()) {
        val iot_table = rs.getString("iot_tbl")
        val iot_field = rs.getString("iot_fld")

        val checkSql1 = s"SELECT ${iot_field}_datasample FROM $iot_table order by time desc limit 10"
        val checkSql2 = s"SELECT ${iot_field}_dataphysics FROM $iot_table"
        val checkSql3 = s"SELECT ${iot_field} FROM $iot_table order by time  limit 10"
        if (timeseriesExists(checkSql1)) {
          num += 1
          buffer += iot_field // 使用 :+= 来追加元素到数组
        }else{
          buffer2+=iot_field
          buffer3+=iot_field+"_datasample"
        }
        if(timeseriesExists(checkSql3)){
          if(buffer.contains(iot_field)){
            buffer1+=iot_field
          }else{
            num2+=1
          }
          num1+=1
        }
      }

      println(buffer2.mkString(","))
      println(buffer.mkString(","))
      println(buffer3.mkString(","))
      println(s"插入的num数据：$num ： num2的值：$num2  总的值：$num1")
    } finally {
      // 确保关闭所有资源
      if (rs != null) rs.close()
      if (pgStmt != null) pgStmt.close()
      if (pgConn != null) pgConn.close()
      if (iotdbStmt != null) iotdbStmt.close()
      if (iotdbConn != null) iotdbConn.close()
    }
  }
}


//插入的num数据：39282 ： num2的值：0  总的值：39282