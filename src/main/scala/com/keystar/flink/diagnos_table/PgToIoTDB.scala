package com.keystar.flink.diagnos_table

import java.sql.{Connection, DriverManager, ResultSet, Statement}

object PgToIoTDB {
  def main(args: Array[String]): Unit = {
    // PostgreSQL连接信息
    val pgUrl2 = "jdbc:postgresql://192.168.5.12:5432/data?serverTimezone=Asia/Shanghai&useSSL=false&allowMultiQueries=true&currentSchema=public"
    val pgUser2 = "postgres"
    val pgPassword2 = "123456"


    val pgUrl = "jdbc:postgresql://192.168.5.12:5432/postgres?serverTimezone=Asia/Shanghai&useSSL=false&allowMultiQueries=true&currentSchema=public"
    val pgUser = "postgres"
    val pgPassword = "123456"


    // IoTDB连接信息
    val iotdbUrl = "jdbc:iotdb://192.168.5.13:6667"
    val iotdbUser = "root"
    val iotdbPassword = "root"

    // 连接到PostgreSQL
    Class.forName("org.postgresql.Driver")
    val pgConn: Connection = DriverManager.getConnection(pgUrl, pgUser, pgPassword)
    val pgStmt: Statement = pgConn.createStatement()

    // 连接到PostgreSQL(写入库)
    Class.forName("org.postgresql.Driver")
    val pgConn2: Connection = DriverManager.getConnection(pgUrl, pgUser, pgPassword)
    val pgStmt2: Statement = pgConn.createStatement()

    // 连接到IoTDB
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver")
    val iotdbConn: Connection = DriverManager.getConnection(iotdbUrl, iotdbUser, iotdbPassword)
    val iotdbStmt: Statement = iotdbConn.createStatement()

    // 读取数据
    val query = """
                  |select iot_tbl ,iot_fld
                  |from public.kr_diagnosis_rules
                  |where iot_tbl !='root.ln.`null`'
                  |and site_id in('1821383836034924544'
                  |,'1821384526656438272'
                  |,'1821384763865300992'
                  |,'1821384903376240640'
                  |,'1821385107949223936'
                  |,'1821385250727526400'
                  |,'1821385428708622336'
                  |,'1821715907433463808'
                  |,'1821716650622189568')
                  |group by iot_tbl ,iot_fld
                  |""".stripMargin  // 只读取一条数据以获取列信息

    val rs: ResultSet = pgStmt.executeQuery(query)


    var num=0
    while (rs.next()){
      val iot_table =rs.getString("iot_tbl")
      val iot_field=rs.getString("iot_fld")
      //打印表名
      val iotsql=s"CREATE TIMESERIES $iot_table.$iot_field"+"_basicerr WITH DATATYPE=TEXT, ENCODING=PLAIN;"
      val iotsql2=s"CREATE TIMESERIES $iot_table.$iot_field"+"_valid WITH DATATYPE=DOUBLE, ENCODING=RLE;"
      // 检查时间序列是否存在
      val checkSql1 = s"SHOW TIMESERIES $iot_table.$iot_field" + "_basicerr"
      val checkSql2 = s"SHOW TIMESERIES $iot_table.$iot_field" + "_valid"
      val str_sql=s"select * from public.kr_diagnosis_rules where iot_tbl ='$iot_table' and iot_fld ='$iot_field'"

      def timeseriesExists(checkSql: String): Boolean = {
        val checkRs: ResultSet = iotdbStmt.executeQuery(checkSql)
        val exists = checkRs.next()
        checkRs.close()
        exists
      }

      if (!timeseriesExists(checkSql1)) {
        iotdbStmt.execute(iotsql)
        println("已经执行完第一条：$iotsql")
      } else {
        println(s"Time series $iot_table.$iot_field" +"_datasample already exists, skipping...")
      }
      if (!timeseriesExists(checkSql2)) {
        iotdbStmt.execute(iotsql2)
        println("已经执行完第二条：$iotsql2")
      } else {
        println(s"Time series $iot_table.$iot_field"+"_dataphysics already exists, skipping...")
      }
      num+=1
      println(s"正在计算的num $num")
    }
    println(num)
    rs.close()
    pgStmt.close()
    pgConn.close()
    // 关闭IoTDB连接
    iotdbStmt.close()
    iotdbConn.close()
  }
}
