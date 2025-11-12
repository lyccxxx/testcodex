package com.keystar.flink.iotdbfunction

import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import org.apache.flink.util.Collector

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.time.Instant
import scala.collection.mutable.ListBuffer

object IotdbFunction {

  def getTotalRows(connection: Connection,tag:Timestamp*): Int = {
    val sql = if (tag.isEmpty) {
      "SELECT COUNT(*) FROM public.kr_diagnosis_rules"
    } else {
      // 提取第一个 Timestamp 参数
      val tagValue = tag.head
      s"SELECT COUNT(*) FROM public.kr_diagnosis_rules WHERE updated_at > '${tagValue}'"
    }
    val stmt = connection.createStatement()
    val rs = stmt.executeQuery(sql)
    var num =0
    if (rs.next()) {
      num=rs.getInt(1)
    } else {
      num=0
    }
    rs.close()
    stmt.close()
    connection.close()
    num
  }
  def getKeyTotalRows(connection: Connection): ListBuffer[Map[String,Int]] = {

    val sql =
      """
        |with a as(select
        |  iot_tbl,
        |  CASE
        |        WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2 THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
        |        ELSE split_part(iot_fld, '_', 1)
        |    END AS iot_dev,count(1) as counts
        |from public.kr_diagnosis_rules
        |group by iot_tbl,iot_dev)
        |select *
        |from a
        |""".stripMargin

    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

    val listBuffer: ListBuffer[Map[String, Int]] = ListBuffer.empty[Map[String, Int]]
    // 设置参数
    val rs = preparedStatement.executeQuery()
    if (rs.next()) {
      val site_code =rs.getString("iot_tbl")+"_"+rs.getString("iot_dev")
      val map1: Map[String, Int] = Map(site_code -> rs.getInt(3))
      listBuffer += map1
    }
    rs.close()
    preparedStatement.close()
    connection.close()
    listBuffer
  }



  def get_SiteIds(connect:Connection,id:Int): ListBuffer[(String, String)] = {
    val table = id match {
      case 1 => s"WHERE iot_tbl NOT IN ('root.ln.`1821385107949223936`','root.ln.`1269014857442188595`','root.ln.`1821385428708622336`') AND iot_fld LIKE 'S001_%'"
      case 2 => "WHERE  iot_tbl not IN ('root.ln.`1821385107949223936`','root.ln.`1269014857442188595`','root.ln.`1821385428708622336`') AND iot_fld NOT LIKE 'S001_%'"
      case 3 => "WHERE iot_tbl IN ('root.ln.`1821385107949223936`','root.ln.`1269014857442188595`','root.ln.`1821385428708622336`') AND iot_fld NOT LIKE 'S001_%'"
      case 4 => "WHERE iot_tbl IN ('root.ln.`1821385107949223936`','root.ln.`1269014857442188595`','root.ln.`1821385428708622336`') AND iot_fld NOT LIKE 'S001_%'"
      case _ => ""
    }

    val column = id match {
      case 1 => "substring(iot_fld,0,9) iot_dev"
      case 2 => "substring(iot_fld,0,6) iot_dev"
      case 4 => "iot_fld as iot_dev  -- substring(iot_fld, 0, 9) AS iot_dev"
      case 3 =>
        """
          |CASE
          |  WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2
          |    THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
          |  ELSE split_part(iot_fld, '_', 1)
          |END AS iot_dev
          |""".stripMargin
      case _ => "substring(iot_fld, 0, 5) AS iot_dev"
    }

    val sql =
      s"""
         |with a as(
         |SELECT
         |  iot_tbl,${column}
         |FROM
         |  public.kr_diagnosis_rules
         | ${table}
         | GROUP BY
         |  iot_tbl, iot_dev
         |)
         |select * from a
         |""".stripMargin
    val stmt = connect.createStatement()
    // 执行查询
    val rs: ResultSet = stmt.executeQuery(sql)
    // 创建空的可变列表
    val emptyBuffer: ListBuffer[(String, String)] = ListBuffer.empty[(String, String)]
    // 遍历结果集并添加数据
    while (rs.next()) {
      emptyBuffer += ((rs.getString("iot_tbl"), rs.getString("iot_dev")))
    }
    // 关闭资源
    rs.close()
    stmt.close()
    emptyBuffer
  }

  def createDiagnosisRuleFromResultSet(rs: ResultSet): DiagnosisRule = {
    DiagnosisRule(
      id = rs.getLong("id"),
      site_id = rs.getString("site_id"),
      iot_tbl = rs.getString("iot_tbl"),
      iot_dev = rs.getString("iot_dev"),
      iot_fld = rs.getString("iot_fld"),
      storage_type = rs.getString("storage_type"),
      src_disconn = rs.getBoolean("src_disconn"),
      samp_freq_mismatch = rs.getBoolean("samp_freq_mismatch"),
      samp_freq_diag_time = rs.getLong("samp_freq_diag_time"),
      samp_freq_clr_time = rs.getLong("samp_freq_clr_time"),
      conv_amp_factor_sign = rs.getBoolean("conv_amp_factor_sign"),
      conv_amp_factor = rs.getDouble("conv_amp_factor"),
      norm_val_sign = rs.getBoolean("norm_val_sign"),
      norm_val = rs.getDouble("norm_val"),
      auto_mon_max_val = rs.getBoolean("auto_mon_max_val"),
      auto_clr_max_val = rs.getBoolean("auto_clr_max_val"),
      max_val_thres = rs.getDouble("max_val_thres"),
      max_val_estab_time = rs.getLong("max_val_estab_time"),
      max_val_clr_time = rs.getLong("max_val_clr_time"),
      auto_mon_min_val = rs.getBoolean("auto_mon_min_val"),
      auto_clr_min_val = rs.getBoolean("auto_clr_min_val"),
      min_val_thres = rs.getDouble("min_val_thres"),
      min_val_estab_time = rs.getLong("min_val_estab_time"),
      min_val_clr_time = rs.getLong("min_val_clr_time"),
      auto_mon_rate_chg = rs.getBoolean("auto_mon_rate_chg"),
      auto_clr_rate_chg = rs.getBoolean("auto_clr_rate_chg"),
      rate_chg_thres = rs.getDouble("rate_chg_thres"),
      rate_chg_estab_time = rs.getLong("rate_chg_estab_time"),
      rate_chg_clr_time = rs.getLong("rate_chg_clr_time"),
      auto_mon_dead_zone = rs.getBoolean("auto_mon_dead_zone"),
      auto_clr_dead_zone = rs.getBoolean("auto_clr_dead_zone"),
      dead_zone_thres_z1 = rs.getDouble("dead_zone_thres_z1"),
      dead_zone_thres_v1 = rs.getDouble("dead_zone_thres_v1"),
      dead_zone_thres_v2 = rs.getDouble("dead_zone_thres_v2"),
      dead_zone_thres_v3 = rs.getDouble("dead_zone_thres_v3"),
      created_at = rs.getTimestamp("created_at"),
      updated_at = rs.getTimestamp("updated_at")
    )
  }

  def get_Connect(): Connection = {
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"

    Class.forName(driver)
    // 创建数据库连接
    val connection = DriverManager.getConnection(url, user, password)
    connection
  }

  def get_Pgcount(): Int = {
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"

    Class.forName(driver)
    // 创建数据库连接
    val connection = DriverManager.getConnection(url, user, password)
    val total = getTotalRows(connection)
    total
  }

  def Diagnostic_method(
                         key: String,
                         value: Option[Any],
                         iotData: IoTDBReading,
                         rule: DiagnosisRule,
                         lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                         samplingFrequencyMismatchStart: Map[String, (Option[Long], Option[Long])],
                         faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                         dedValidValues: Map[String, List[(Option[Double], Some[Long])]]
                       ): (Map[String, (Option[Any], Option[Long], Option[Double])],
    Map[String, (Option[Long], Option[Long])],
    Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
    Map[String, List[(Option[Double], Some[Long])]],Boolean) = {

    if (rule.samp_freq_mismatch) {
      val (updatedLastValidValues, updatedMismatchStart, sampCheck) =
        checkSamplingFrequency(key, value, iotData, rule, lastValidValues, samplingFrequencyMismatchStart)
//      println(s"打印采集异常判定：${iotData.timestamp} $sampCheck")
      val boolvalue=isFalseOrTrue(value)
      if (!sampCheck && !boolvalue) {
        val va_num = toDouble(value)
        val (updatedLastValidValues2, updatedFaultStatesMap, updatedDedValidValues) =
          handleValidSampling(key, va_num, iotData, rule, updatedLastValidValues, faultStatesMap, dedValidValues)

        (updatedLastValidValues2, updatedMismatchStart, updatedFaultStatesMap, updatedDedValidValues,sampCheck)
      } else {
//          val newLastValidValues = updatedLastValidValues - key
          val newMismatchStart = updatedMismatchStart - key
          val newFaultStatesMap = faultStatesMap - key
          val newDedValidValues = dedValidValues - key
//          println(s"打印删除之后的死值：${iotData.timestamp} $newDedValidValues")
          (updatedLastValidValues, newMismatchStart, newFaultStatesMap, newDedValidValues, sampCheck)
          //        else {
//          (updatedLastValidValues, updatedMismatchStart, faultStatesMap, dedValidValues, sampCheck)
//        }
      }
    } else {
      val boolvalue=isFalseOrTrue(value)
      if(!boolvalue){
        val va_num = toDouble(value)
        val (updatedLastValidValues2, updatedFaultStatesMap, updatedDedValidValues) =
          handleValidSampling(key, va_num, iotData, rule, lastValidValues, faultStatesMap, dedValidValues)
        (updatedLastValidValues2, samplingFrequencyMismatchStart, updatedFaultStatesMap, updatedDedValidValues,false)
      }else{
        //可能还需要输出布尔类型的数值
        // 更新 lastValidValues
        //        val updatedLastValidValues = lastValidValues + (key -> (value, Some(iotData.timestamp), None))
        val updatedLastValidValues=updateLastValue(key,None,Some(iotData.timestamp),None,lastValidValues,shouldUpdateTimestamp = true)
        (updatedLastValidValues, samplingFrequencyMismatchStart, faultStatesMap, dedValidValues,false)
      }
    }
  }

  def checkSamplingFrequency(
                              key: String,
                              value: Option[Any],
                              iotData: IoTDBReading,
                              rule: DiagnosisRule,
                              lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                              mismatchStart: Map[String, (Option[Long],Option[Long])]
                            ): (Map[String, (Option[Any], Option[Long], Option[Double])], Map[String, (Option[Long],Option[Long])], Boolean) = {

    //Map[String, (Option[Long],Option[Long])]
    val lastTimestamp = lastValidValues.getOrElse(key, (None, None, None))._2
    //    val updatedLastValidValues = lastValidValues + (key -> (value, Some(iotData.timestamp), None))
    val updatedLastValidValues=updateLastValue(key,None,Some(iotData.timestamp),None,lastValidValues,shouldUpdateTimestamp = true)

    if (lastTimestamp.isDefined) {
      val actualSamplingInterval = (iotData.timestamp - lastTimestamp.get) / 1000 // 转换为秒

//      println(s"打印上一个值：$lastTimestamp 和 现在的值:${iotData.timestamp} 加上：$actualSamplingInterval")
      // 检查是否采样频率不符
      if (actualSamplingInterval > 15) {
        val startTimestamp = mismatchStart.getOrElse(key, (None, None))._1

        // 如果问题尚未开始记录，并且当前间隔小于诊断持续时间
        if (startTimestamp.isEmpty && actualSamplingInterval < rule.samp_freq_diag_time) {
          val updatedMismatchStart = mismatchStart + (key -> (Some(iotData.timestamp), None))
          return (updatedLastValidValues, updatedMismatchStart, false)
        } else {
          // 检查是否达到诊断持续时间
          if (actualSamplingInterval >= rule.samp_freq_diag_time ||
            (startTimestamp.nonEmpty && ((iotData.timestamp - startTimestamp.get) / 1000 >= rule.samp_freq_diag_time))) {
            //因为startTimestamp 没有及时的被清楚导致失败
            return (updatedLastValidValues, mismatchStart, true)
          }
        }
      } else {
        // 检查是否采样频率恢复正常
        val startTimestamp = mismatchStart.getOrElse(key, (None,None))._1
        if(actualSamplingInterval==15){
          val updatedMismatchStart = mismatchStart - key
          return (updatedLastValidValues, updatedMismatchStart, false)
        }else if (startTimestamp.isDefined) {
//          println(s"打印故障开始时间和结束时间：$startTimestamp ${iotData.timestamp}")
          val clrstartTimestamp = mismatchStart.getOrElse(key, (None,None))._2
          if (clrstartTimestamp.isEmpty&&actualSamplingInterval<rule.samp_freq_clr_time&&actualSamplingInterval>15) {
            // 记录问题清除开始的时间戳
            //            out.collect(DiagnosisResult(iotData.timestamp, key,value,"0b010000",lastValidValues.getOrElse(key, (None, None, None))._1,lastValidValues.getOrElse(key, (None, None, None))._2)) // 输出结果
            val updatedClrmatchStart = mismatchStart + (key -> (startTimestamp,Some(iotData.timestamp)))
            return (updatedLastValidValues, updatedClrmatchStart, true)
          } else {
            // 检查持续时间
            if ( actualSamplingInterval>= rule.samp_freq_clr_time) {
              val updatedMismatchStart = mismatchStart - key
              return (updatedLastValidValues, updatedMismatchStart, false)
            }else if(clrstartTimestamp.nonEmpty&&((iotData.timestamp - clrstartTimestamp.get) / 1000 >= rule.samp_freq_clr_time)) {
              val updatedMismatchStart = mismatchStart - key
              return (updatedLastValidValues, updatedMismatchStart, false)
            }
          }
        }
      }
    }
    // 更新 lastValidValues
    (updatedLastValidValues, mismatchStart, false)
  }

  def handleValidSampling(
                           key: String,
                           value: Option[Double],
                           iotData: IoTDBReading,
                           rule: DiagnosisRule,
                           lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                           faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                           dedValidValues: Map[String, List[(Option[Double], Some[Long])]]
                         ): (Map[String, (Option[Any], Option[Long], Option[Double])],
    Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
    Map[String, List[(Option[Double], Some[Long])]]) = {

    // 获取上次的有效值和时间戳
    val (lastValue, lastTimestamp, _) = lastValidValues.getOrElse(key, (None, None, None))

    // 系数转换
    val outValue = value.map { v =>
      val convFactor = if (rule.conv_amp_factor_sign) rule.conv_amp_factor else 1.0
      v / convFactor
    }


    val physicalValue = if(rule.norm_val_sign)outValue.map(data => (data * rule.norm_val).asInstanceOf[Double]) else outValue

    // 更新 lastValidValues
    //    val updatedLastValidValues = lastValidValues + (key -> (value, Some(iotData.timestamp), physicalValue))
    var updatedLastValidValues=lastValidValues
    var dedValue: Map[String, List[(Option[Double], Some[Long])]]=dedValidValues
    var updatedFaultStatesMap=faultStatesMap
    var finalResult2=false

    // 极大值故障
    val (maxFault, maxFaultStart, maxFaultRecover, minFault, minFaultStart, minFaultRecover, rateFault, rateFaultStart, rateFaultRecover, deadValueFault, hard_result) =
      faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))

    if(maxFault){
      val maxStateValue  = processMaxValueFault(key, physicalValue, rule, faultStatesMap, iotData)
      val minStateValue=processMinValueFault(key, physicalValue, rule, maxStateValue._2, iotData)
      //进行变化值故障测试
      val filteredValues = filterLastValidValues(lastValidValues)
      val RateStatesMap = processRateValueFault(key, physicalValue, filteredValues, rule, minStateValue._2, iotData)
      val DeadStatesMap = processDeadValueFault(key, physicalValue, dedValidValues, rule, RateStatesMap._2, iotData)
      val finalResult5 = RateStatesMap._1||maxStateValue._1||minStateValue._1||DeadStatesMap._1
      if(!finalResult5){
        val checkvalue=checkValidValue(key,physicalValue,iotData,rule,lastValidValues, dedValidValues)
        val num =toDouble(physicalValue)
        updatedLastValidValues=if(checkvalue)updateLastValue(key,num,Some(iotData.timestamp),None,lastValidValues, shouldUpdateValue = true, shouldUpdateTimestamp = true) else updatedLastValidValues
      }
      dedValue=DeadStatesMap._2
      val updates = buildUpdates(
        newHardResult = Some(finalResult5),
        newDeadValueFault = Some(DeadStatesMap._1)
      )
      val updatedFaultStatesMap5 = updateFaultStatesMap(key, updates, minStateValue._2)
      updatedFaultStatesMap = updatedFaultStatesMap5
//      println(s"打印极大值的死值结果：$dedValue")
    }else if(minFault){

      val minStateValue=processMinValueFault(key, physicalValue, rule, faultStatesMap, iotData)
      val maxStateValue  = processMaxValueFault(key, physicalValue, rule, minStateValue._2, iotData)
      val filteredValues = filterLastValidValues(lastValidValues)
      val RateStatesMap = processRateValueFault(key, physicalValue, filteredValues, rule, maxStateValue._2, iotData)
      val DeadStatesMap = processDeadValueFault(key, physicalValue, dedValidValues, rule, RateStatesMap._2, iotData)
      val finalResult5 = RateStatesMap._1||maxStateValue._1||minStateValue._1||DeadStatesMap._1
      if(!finalResult5){
        val checkvalue=checkValidValue(key,physicalValue,iotData,rule,lastValidValues, dedValidValues)
        val num =toDouble(physicalValue)
        updatedLastValidValues=if(checkvalue)updateLastValue(key,num,Some(iotData.timestamp),None,lastValidValues,shouldUpdateTimestamp = true,shouldUpdateValue = true) else updatedLastValidValues
      }
      dedValue=DeadStatesMap._2
      val updates = buildUpdates(
        newHardResult = Some(finalResult5),
        newDeadValueFault = Some(DeadStatesMap._1)
      )
      val updatedFaultStatesMap5 = updateFaultStatesMap(key, updates, RateStatesMap._2)
      updatedFaultStatesMap = updatedFaultStatesMap5
//      println(s"打印极小值的死值结果：$dedValue")
    }else if(rateFault){
      //进行变化值故障测试
      val filteredValues = filterLastValidValues(lastValidValues)
      val RateStatesMap = processRateValueFault(key, physicalValue, filteredValues, rule, faultStatesMap, iotData)
      val maxStateValue  = processMaxValueFault(key, physicalValue, rule, RateStatesMap._2, iotData)
      val minStateValue=processMinValueFault(key, physicalValue, rule, maxStateValue._2, iotData)
      val DeadStatesMap = processDeadValueFault(key, physicalValue, dedValidValues, rule, minStateValue._2, iotData)
      val finalResult5 = RateStatesMap._1||maxStateValue._1||minStateValue._1||DeadStatesMap._1
      if(!finalResult5){
        val checkvalue=checkValidValue(key,physicalValue,iotData,rule,lastValidValues, dedValidValues)
        val num =toDouble(physicalValue)
        updatedLastValidValues=if(checkvalue)updateLastValue(key,num,Some(iotData.timestamp),None,lastValidValues,shouldUpdateTimestamp = true,shouldUpdateValue = true) else updatedLastValidValues
      }
      dedValue=DeadStatesMap._2
      val updates = buildUpdates(
        newHardResult = Some(finalResult5),
        newDeadValueFault = Some(DeadStatesMap._1)
      )

      val updatedFaultStatesMap5 = updateFaultStatesMap(key, updates, minStateValue._2)
      updatedFaultStatesMap = updatedFaultStatesMap5
//      println(s"打印变化率值的死值结果：$dedValue")
    }else if(deadValueFault){
      val DeadStatesMap = processDeadValueFault(key, physicalValue, dedValidValues, rule, faultStatesMap, iotData)

      val maxStateValue  = processMaxValueFault(key, physicalValue, rule, faultStatesMap, iotData)
      val minStateValue=processMinValueFault(key, physicalValue, rule, maxStateValue._2, iotData)
      val filteredValues = filterLastValidValues(lastValidValues)
      val RateStatesMap = processRateValueFault(key, physicalValue,filteredValues, rule, minStateValue._2, iotData)

      val finalResult5 = RateStatesMap._1||maxStateValue._1||minStateValue._1||DeadStatesMap._1
      dedValue=DeadStatesMap._2
      val updates = buildUpdates(
        newHardResult = Some(finalResult5),
        newDeadValueFault = Some(DeadStatesMap._1)
      )
      if(!finalResult5){
        val checkvalue=checkValidValue(key,physicalValue,iotData,rule,lastValidValues, dedValidValues)
        val num =toDouble(physicalValue)
        updatedLastValidValues=if(checkvalue)updateLastValue(key,num,Some(iotData.timestamp),None,lastValidValues,shouldUpdateTimestamp = true,shouldUpdateValue = true) else updatedLastValidValues
      }
      val updatedFaultStatesMap5 = updateFaultStatesMap(key, updates, RateStatesMap._2)
      updatedFaultStatesMap = updatedFaultStatesMap5
    }else{
      val noramlStateVlaue =Verifivalue(key, physicalValue, rule, iotData, lastValidValues, faultStatesMap, dedValidValues)
      updatedLastValidValues=noramlStateVlaue._1
      updatedFaultStatesMap=noramlStateVlaue._2
      dedValue=noramlStateVlaue._3
    }
    updatedLastValidValues=updateLastValue(key,None,Some(iotData.timestamp),physicalValue,updatedLastValidValues,shouldUpdateTimestamp = true,shouldUpdatePhysicalValue = true)

    // 返回更新后的值
//    println(s"打印返回的结果值：$dedValue")
    (updatedLastValidValues, updatedFaultStatesMap, dedValue)
  }

  def Verifivalue(
                   key: String,
                   physicalValue: Option[Double],
                   rule: DiagnosisRule,
                   iotData: IoTDBReading,
                   lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                   faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                   dedValidValues: Map[String, List[(Option[Double], Some[Long])]]
                 ): (Map[String, (Option[Any], Option[Long], Option[Double])],
    Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
    Map[String, List[(Option[Double], Some[Long])]]) = {

    var updatedFaultStatesMap=faultStatesMap
    var updatedLastValidValues=lastValidValues
    var dedValue=dedValidValues
    val (maxResult, updatedMaxStatesMap) = processMaxValueFault(key, physicalValue, rule, faultStatesMap, iotData)
    if (!maxResult) {

      // 极小值故障
      var (minResult, updatedMinStatesMap) = processMinValueFault(key, physicalValue, rule, updatedMaxStatesMap, iotData)
      if (!minResult){
        // 变化率故障
        val filteredValues = filterLastValidValues(lastValidValues)
        var (rateResult, updatedRateStatesMap) = processRateValueFault(key, physicalValue, filteredValues, rule, updatedMinStatesMap, iotData)
        if (!rateResult){
          // 死值故障
          var (deadResult, updatedDeadStatesMap) = processDeadValueFault(key, physicalValue, dedValidValues, rule, updatedRateStatesMap, iotData)
//          println(s"返回得到的死值的结果值： ${Thread.currentThread().getName} ${iotData.timestamp}  $updatedDeadStatesMap $deadResult $updatedRateStatesMap")
          dedValue=updatedDeadStatesMap
          if (!deadResult) {
            // 硬故障
            val finalResult5: Boolean = (maxResult || minResult || rateResult || deadResult)
            // 有效值判断
            val checkvalue=checkValidValue(key,physicalValue,iotData,rule,lastValidValues, dedValidValues)
            val num =toDouble(physicalValue)
            updatedLastValidValues=if(checkvalue)updateLastValue(key,num,Some(iotData.timestamp),None,lastValidValues,shouldUpdateTimestamp = true,shouldUpdateValue = true) else updatedLastValidValues
            // 更新故障状态
            val updates = buildUpdates(
              newHardResult = Some(finalResult5),
              newDeadValueFault = Some(deadResult)
            )
            val updatedFaultStatesMap5 = updateFaultStatesMap(key, updates, updatedRateStatesMap)
            updatedFaultStatesMap = updatedFaultStatesMap5
          }else{
            val updates = buildUpdates(
              newHardResult = Some(deadResult),
              newDeadValueFault = Some(deadResult)
            )
            val updatedFaultStatesMap44 = updateFaultStatesMap(key, updates, updatedRateStatesMap)
//            print(s"打印死值更新map集合： 更新前：$updatedRateStatesMap 更新后：$updatedFaultStatesMap44 ${iotData.timestamp}")
            updatedFaultStatesMap = updatedFaultStatesMap44
          }
        }else{
          val updates = buildUpdates(
            newHardResult = Some(rateResult)
          )
          val updatedFaultStatesMap33 = updateFaultStatesMap(key, updates, updatedRateStatesMap)
          updatedFaultStatesMap = updatedFaultStatesMap33
          val dedValidValue =updateDedValidValues(key, physicalValue, iotData.timestamp, rule, dedValidValues)
          dedValue=dedValidValue
        }
      }else{
        val updates = buildUpdates(
          newHardResult = Some(minResult)
        )
        val updatedFaultStatesMap22 = updateFaultStatesMap(key, updates, updatedMinStatesMap)
        updatedFaultStatesMap = updatedFaultStatesMap22
        val dedValidValue =updateDedValidValues(key, physicalValue, iotData.timestamp, rule, dedValidValues)
        dedValue=dedValidValue
      }
    }else{
      val updates = buildUpdates(
        newHardResult = Some(maxResult)
      )
      val updatedFaultStatesMap1 = updateFaultStatesMap(key, updates, updatedMaxStatesMap)
      updatedFaultStatesMap = updatedFaultStatesMap1
//      println("进入else")
      val dedValidValue =updateDedValidValues(key, physicalValue, iotData.timestamp, rule, dedValidValues)
      dedValue=dedValidValue
    }
    (updatedLastValidValues,updatedFaultStatesMap,dedValue)
  }

  def checkValidValue(
                       key: String,
                       physicalValue: Option[Double],
                       iotData: IoTDBReading,
                       rule: DiagnosisRule,
                       lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                       dedValidValues: Map[String, List[(Option[Double], Some[Long])]]
                     ): Boolean = {

    // 获取上一个物理值
    val lastValueOpt = lastValidValues.get(key).flatMap(_._3)

    // 计算变化率，并确保在计算前物理值和当前值都存在
    val rateOpt = for {
      lastValue <- lastValueOpt
      currentValue <- physicalValue
    } yield math.abs(currentValue - lastValue)

    // 检查物理值是否在有效范围内并且变化率满足阈值
    val checkNum = physicalValue.exists(value =>
      value > rule.min_val_thres && value < rule.max_val_thres && rateOpt.exists(_ < rule.rate_chg_thres)
    )

    // 获取上次检查的时间点
    val currentTime = iotData.timestamp
    val lastCheckTimePoint = dedValidValues.get(key).flatMap(_.headOption.map(_._2.getOrElse(0L)))

    // 计算自上次检查以来的时间差（秒）
    val timeSinceLastCheck = lastCheckTimePoint.map(lastTime => (currentTime - lastTime) / 1000).getOrElse(0L)

    // 更新最近的有效值列表
    val updatedLastValidValues = dedValidValues.getOrElse(key, Nil)

    // 检查死区条件
    val checkNum2 = if (timeSinceLastCheck >= rule.dead_zone_thres_z1) {
      // 获取窗口数据并排序
      val windowData = updatedLastValidValues.flatMap(_._1).toList.sorted

      if (windowData.nonEmpty) {
        val minValue = windowData.head
        val maxValue = windowData.last
        val medianIndex = windowData.length / 2
        val median = windowData(medianIndex)
        val secondMax = if (windowData.length > 1) windowData.dropRight(1).last else minValue
        val secondMin = if (windowData.length > 1) windowData(1) else minValue

        val condition1 = math.abs(maxValue - minValue) <= rule.dead_zone_thres_v1
        val condition2 = math.abs(median - secondMax) <= rule.dead_zone_thres_v2
        val condition3 = math.abs(median - secondMin) <= rule.dead_zone_thres_v3

        condition1 || (condition2 && condition3)
      } else {
        true
      }
    } else {
      true
    }

    // 返回最终结果
    checkNum && checkNum2
  }

  def updateLastValue(
                       key: String,
                       newValue: Option[Double],
                       newTimestamp: Option[Long],
                       newPhysicalValue: Option[Double],
                       lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                       shouldUpdateValue: Boolean = false,
                       shouldUpdateTimestamp: Boolean = false,
                       shouldUpdatePhysicalValue: Boolean = false
                     ): Map[String, (Option[Any], Option[Long], Option[Double])] = {

    // 获取当前的条目或默认值
    val (oldValue, oldTimestamp, oldPhysicalValue) = lastValidValues.getOrElse(key, (None, None, None))

    // 根据条件选择性更新各个字段
    val updatedValue = if (shouldUpdateValue) newValue else oldValue
    val updatedTimestamp = if (shouldUpdateTimestamp) newTimestamp else oldTimestamp
    val updatedPhysicalValue = if (shouldUpdatePhysicalValue) newPhysicalValue else oldPhysicalValue

    // 更新映射
    lastValidValues + (key -> (updatedValue, updatedTimestamp, updatedPhysicalValue))
  }


  def filterLastValidValues(lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])]): Map[String, (Option[Double], Option[Long], Option[Double])] = {
    lastValidValues.collect { case (key, (Some(value: Double), timestamp, doubleValue)) =>
      (key, (Some(value), timestamp, doubleValue))
    }
  }

  def processMaxValueFault(
                            key: String,
                            physicalValue: Option[Double],
                            rule: DiagnosisRule,
                            faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                            iotData: IoTDBReading
                          ): (Boolean,Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]) = {
    var establishMaxvalue = false
    var clearMaxvalue = false
    var finalResult = false
    var updatefaultStatesMap=Map.empty[String,(Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]
    // 用户开启极大值自动监测功能
    if (rule.auto_mon_max_val) {
      val (establishMaxvalue,newMaxFaultStart,newMaxFaultRecover) = establishMaxFault(key, physicalValue, rule, faultStatesMap, iotData)
      //更新里边的值
      val newMaxFaultState=Some(establishMaxvalue,newMaxFaultStart,newMaxFaultRecover)
      val updates = buildUpdates(
        newMaxFaultState = newMaxFaultState,
        newHardResult = Some(establishMaxvalue)
      )
      updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
      finalResult = establishMaxvalue
      if (establishMaxvalue) {
        // 用户开启自动清除功能
        if (rule.auto_clr_max_val && physicalValue.exists(_ <= rule.max_val_thres)) {
          val (clearMaxvalue2,newMaxFaultStart,newMaxFaultRecover)= clearMaxFault(key, updatefaultStatesMap, iotData, rule)
          clearMaxvalue=clearMaxvalue2
          //更新faultStatesMap
          val newMaxFaultState=Some(clearMaxvalue2,newMaxFaultStart,newMaxFaultRecover)
          val updates = buildUpdates(
            newMaxFaultState = newMaxFaultState,
            newHardResult = Some(clearMaxvalue)
          )
          updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
//          println(s"极大值故障清除结果: $clearMaxvalue")
          finalResult = clearMaxvalue
        }
      }
    } else {
//      println("极大值自动监测功能未开启")
      if(rule.auto_clr_max_val && physicalValue.exists(_ <= rule.max_val_thres) ){
        val (clearMaxvalue2,newMaxFaultStart,newMaxFaultRecover)= clearMaxFault(key, faultStatesMap, iotData, rule)
        clearMaxvalue=clearMaxvalue2
        //更新faultStatesMap
        val newMinFaultState=Some(clearMaxvalue2,newMaxFaultStart,newMaxFaultRecover)
        val updates = buildUpdates(
          newMaxFaultState = newMinFaultState,
          newHardResult = Some(clearMaxvalue)
        )
        updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
//        println(s"极大值故障清除结果: $clearMaxvalue")
        finalResult = clearMaxvalue
      }
    }
    (finalResult,updatefaultStatesMap)
  }

  // 处理极小值故障的函数
  def processMinValueFault(
                            key: String,
                            physicalValue: Option[Double],
                            rule: DiagnosisRule,
                            faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                            iotData: IoTDBReading
                          ): (Boolean,Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]) = {
    var establishMinvalue = false
    var clearMinvalue = false
    var finalResult = false
    var updatefaultStatesMap=Map.empty[String,(Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]
    // 用户开启极小值自动监测功能
    if (rule.auto_mon_min_val) {
      val (establishMinvalue,newMinFaultStart,newMinFaultRecover) = establishMinFault(key, physicalValue, rule, faultStatesMap, iotData)
      //更新里边的值
      val newMinFaultState=Some(establishMinvalue,newMinFaultStart,newMinFaultRecover)
      val updates = buildUpdates(
        newMinFaultState = newMinFaultState,
        newHardResult = Some(establishMinvalue)
      )
      updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
      finalResult=establishMinvalue
      if (establishMinvalue) {
//        println(s"极小值故障已检测到: $establishMinvalue")
        // 用户开启自动清除功能
        if (rule.auto_clr_min_val && physicalValue.exists(_ > rule.min_val_thres)) {
          val (clearMinvalue2,newMinFaultStart,newMinFaultRecover)= clearMinFault(key, faultStatesMap, iotData, rule)
          clearMinvalue=clearMinvalue2
          //更新faultStatesMap
          val newMinFaultState=Some(clearMinvalue2,newMinFaultStart,newMinFaultRecover)
          val updates = buildUpdates(
            newMinFaultState = newMinFaultState,
            newHardResult = Some(clearMinvalue)
          )
          updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
//          println(s"极小值故障清除结果: $clearMinvalue")
          finalResult =clearMinvalue
        }
      }
    } else {
//      println("极小值自动监测功能未开启")
      if(rule.auto_clr_min_val && physicalValue.exists(_ > rule.min_val_thres)){
        val (clearMinvalue2,newMinFaultStart,newMinFaultRecover)= clearMinFault(key, faultStatesMap, iotData, rule)
        clearMinvalue=clearMinvalue2
        //更新faultStatesMap
        val newMinFaultState=Some(clearMinvalue2,newMinFaultStart,newMinFaultRecover)
        val updates = buildUpdates(
          newMinFaultState = newMinFaultState,
          newHardResult = Some(clearMinvalue)
        )
        updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
//        println(s"极小值故障清除结果: $clearMinvalue")
      }
      finalResult = if (clearMinvalue) true else false
    }
    (finalResult,updatefaultStatesMap)
  }

  //处理变化率故障的函数
  def processRateValueFault(
                             key: String,
                             physicalValue: Option[Double],
                             lastPhysicalValue: Map[String, (Option[Double], Option[Long], Option[Double])],
                             rule: DiagnosisRule,
                             faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                             iotData: IoTDBReading
                           ): (Boolean,Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]) = {
    var establishRatevalue = false
    var clearRatevalue = false
    var finalResult = false
    var updatefaultStatesMap=faultStatesMap
    // 用户开启变化率自动监测功能
    if (rule.auto_mon_rate_chg) {
      val (establishRatevalue,newRateFaultStart,newRateFaultRecover) = establishRateFault(key, physicalValue,lastPhysicalValue, rule, faultStatesMap, iotData)
      //更新里边的值
      val newRateFaultState=Some(establishRatevalue,newRateFaultStart,newRateFaultRecover)
      val updates = buildUpdates(
        newRateFaultState = newRateFaultState,
        newHardResult = Some(establishRatevalue)
      )
      updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)

      finalResult = establishRatevalue
      if (establishRatevalue) {
//        println(s"变化率故障已检测到: $establishRatevalue")
        // 用户开启自动清除功能
        if (rule.auto_clr_rate_chg) {
          val (clearRatevalue2,newRateFaultStart,newRateFaultRecover)= clearRateFault(key,physicalValue,lastPhysicalValue, rule,faultStatesMap, iotData)
          clearRatevalue=clearRatevalue2
          //更新faultStatesMap
          val newRateFaultState=Some(clearRatevalue2,newRateFaultStart,newRateFaultRecover)
          val updates = buildUpdates(
            newRateFaultState = newRateFaultState,
            newHardResult = Some(clearRatevalue)
          )
          updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
//          println(s"变化率故障清除结果: $clearRatevalue")
          finalResult = clearRatevalue
        }
      }
    } else {
//      println("变化率自动监测功能未开启")
      if(rule.auto_clr_rate_chg){
        val (clearRatevalue2,newRateFaultStart,newRateFaultRecover)= clearRateFault(key,physicalValue,lastPhysicalValue,rule, faultStatesMap, iotData)
        clearRatevalue=clearRatevalue2
        //更新faultStatesMap
        val newMinFaultState=Some(clearRatevalue2,newRateFaultStart,newRateFaultRecover)
        val updates = buildUpdates(
          newMinFaultState = newMinFaultState,
          newHardResult = Some(clearRatevalue)
        )
        updatefaultStatesMap=updateFaultStatesMap(key,updates,faultStatesMap)
//        println(s"极大值故障清除结果: $clearRatevalue")
      }
      finalResult = if (clearRatevalue) true else false
    }
    (finalResult,updatefaultStatesMap)
  }

  //处理死值故障的函数
  def processDeadValueFault(
                             key: String,
                             physicalValue: Option[Double],
                             dedValidValues: Map[String, List[(Option[Double], Some[Long])]],
                             rule: DiagnosisRule,
                             faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                             iotData: IoTDBReading
                           ):  (Boolean,Map[String, List[(Option[Double], Some[Long])]]) = {


    var clearDeadvalue = false
    var finalResult = false
    var dedValidValue=dedValidValues
    // 用户开启死值自动监测功能
    if (rule.auto_mon_dead_zone) {
      val(establishDeadvalue,dedValidValue1) = diagnoseDeadValueFault(key, physicalValue, rule,dedValidValues, faultStatesMap, iotData)

      //更新里边的值
      dedValidValue=dedValidValue1

      finalResult = establishDeadvalue
//      println(s"死值故障已检测到: $establishDeadvalue")
      if (establishDeadvalue) {
        // 用户开启自动清除功能
        if (rule.auto_clr_dead_zone) {
          val (clearRatevalue2,dedValidValue2)= clearDeadValueFault(key,physicalValue, rule,dedValidValues,faultStatesMap, iotData)
          clearDeadvalue=clearRatevalue2
          dedValidValue=dedValidValue2

          finalResult = clearDeadvalue
        }
      }
    } else {
//      println("死值自动监测功能未开启")
      if(rule.auto_clr_dead_zone){
        val (clearRatevalue2,dedValidValue2)= clearDeadValueFault(key,physicalValue, rule,dedValidValue,faultStatesMap, iotData)
        clearDeadvalue=clearRatevalue2
        dedValidValue=dedValidValue2
      }
      finalResult = if (clearDeadvalue) true else false
    }
    (finalResult,dedValidValue)
  }


  //更新faultStatesMap
  def buildUpdates(
                    newMaxFaultState: Option[(Boolean, Option[Long], Option[Long])] = None,
                    newMinFaultState: Option[(Boolean, Option[Long], Option[Long])] = None,
                    newRateFaultState: Option[(Boolean, Option[Long], Option[Long])] = None,
                    newDeadValueFault: Option[Boolean] = None,
                    newHardResult: Option[Boolean] = None
                  ): (Option[Boolean], Option[Long], Option[Long], Option[Boolean], Option[Long], Option[Long], Option[Boolean], Option[Long], Option[Long], Option[Boolean], Option[Boolean]) = {
    val (newMaxFault, newMaxFaultStart, newMaxFaultRecover) = newMaxFaultState.map { case (b, o1, o2) => (Some(b), o1, o2) }.getOrElse((None, None, None))
    val (newMinFault, newMinFaultStart, newMinFaultRecover) = newMinFaultState.map { case (b, o1, o2) => (Some(b), o1, o2) }.getOrElse((None, None, None))
    val (newRateFault, newRateFaultStart, newRateFaultRecover) = newRateFaultState.map { case (b, o1, o2) => (Some(b), o1, o2) }.getOrElse((None, None, None))

    (
      newMaxFault, newMaxFaultStart, newMaxFaultRecover,
      newMinFault, newMinFaultStart, newMinFaultRecover,
      newRateFault, newRateFaultStart, newRateFaultRecover,
      newDeadValueFault, newHardResult
    )
  }

  def updateFaultStatesMap(
                            key: String,
                            updates: (
                              Option[Boolean], Option[Long], Option[Long], // maxFault
                                Option[Boolean], Option[Long], Option[Long], // minFault
                                Option[Boolean], Option[Long], Option[Long], // rateFault
                                Option[Boolean], Option[Boolean] // deadValueFault, hard_result
                              ),
                            faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]
                          ): Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)] = {

    faultStatesMap.get(key) match {
      case Some((oldMaxFault, oldMaxFaultStart, oldMaxFaultRecover, oldMinFault, oldMinFaultStart, oldMinFaultRecover, oldRateFault, oldRateFaultStart, oldRateFaultRecover, oldDeadValueFault, oldHardResult)) =>
        val (newMaxFault, newMaxFaultStart, newMaxFaultRecover) =
          if (updates._1.isDefined && updates._2.isEmpty && updates._3.isEmpty) {
            (updates._1.getOrElse(oldMaxFault),
              updates._2, // 强制更新时间戳为 Some
              updates._3) // 强制更新时间戳为 Some
          } else {
            (updates._1.getOrElse(oldMaxFault),
              updates._2.orElse(oldMaxFaultStart), // 强制更新时间戳为 Some
              updates._3.orElse(oldMaxFaultRecover))
          }
        val (newMinFault, newMinFaultStart, newMinFaultRecover) =
          if(updates._4.isDefined && updates._5.isEmpty && updates._6.isEmpty){
            (updates._4.getOrElse(oldMinFault),
              updates._5, // 强制更新时间戳为 Some
              updates._6) // 强制更新时间戳为 Some
          } else {
            (updates._4.getOrElse(oldMinFault),
              updates._5.orElse(oldMinFaultStart), // 强制更新时间戳为 Some
              updates._6.orElse(oldMinFaultRecover))
          }
        val (newRateFault:Boolean, newRateFaultStart:Option[Long], newRateFaultRecover:Option[Long]) =
          if(updates._7.isDefined && updates._8.isEmpty && updates._9.isEmpty){
            (updates._7.getOrElse(oldRateFault),
              updates._8, // 强制更新时间戳为 Some
              updates._9)
          }else{
            (updates._7.getOrElse(oldRateFault),
              updates._8.orElse(oldRateFaultStart),
              updates._9.orElse(oldRateFaultRecover))
          }

        val (newDeadValueFault:Boolean, newHardResult:Boolean) = (
          updates._10.getOrElse(oldDeadValueFault),
          updates._11.getOrElse(oldHardResult)
        )

        faultStatesMap + (key -> (newMaxFault, newMaxFaultStart, newMaxFaultRecover, newMinFault, newMinFaultStart, newMinFaultRecover, newRateFault, newRateFaultStart, newRateFaultRecover, newDeadValueFault, newHardResult))
      case None =>
        val (newMaxFault:Boolean, newMaxFaultStart:Option[Long], newMaxFaultRecover:Option[Long]) = (
          updates._1.getOrElse(false),
          updates._2.orElse(None),
          updates._3.orElse(None)
        )
        val (newMinFault:Boolean, newMinFaultStart:Option[Long], newMinFaultRecover:Option[Long]) = (
          updates._4.getOrElse(false),
          updates._5.orElse(None),
          updates._6.orElse(None)
        )
        val (newRateFault:Boolean, newRateFaultStart:Option[Long], newRateFaultRecover:Option[Long]) = (
          updates._7.getOrElse(false),
          updates._8.orElse(None),
          updates._9.orElse(None)
        )
        val (newDeadValueFault:Boolean, newHardResult:Boolean) = (
          updates._10.getOrElse(false),
          updates._11.getOrElse(false)
        )

        faultStatesMap + (key -> (newMaxFault, newMaxFaultStart, newMaxFaultRecover, newMinFault, newMinFaultStart, newMinFaultRecover, newRateFault, newRateFaultStart, newRateFaultRecover, newDeadValueFault, newHardResult))
    }
  }
  def establishMaxFault(
                         key: String,
                         physicalValue: Option[Double],
                         rule: DiagnosisRule,
                         faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                         iotData: IoTDBReading
                       ): (Boolean, Option[Long], Option[Long]) = {

    // 从故障状态映射中获取当前键的状态
    val (maxFault, maxFaultStart, maxFaultRecover, _, _, _, _, _, _, _, hard_result) = faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))
    // 硬故障--无效
    if (!maxFault) {
      // 物理值是否大于极大值
      if (physicalValue.exists(_ > rule.max_val_thres)) {
        if (maxFaultStart.isEmpty) {
          // 首次检测到故障
          //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault detected at ${iotData.timestamp}")
          (false, Some(iotData.timestamp), None)
        } else {
          // 计算故障持续时间
          val faultDuration = (iotData.timestamp - maxFaultStart.get) / 1000
          if (faultDuration >= rule.max_val_estab_time) {
            // 故障确认
            //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault confirmed at ${iotData.timestamp}")
            (true,maxFaultStart , None)
          }else{
            // 仍在怀疑阶段
            (false,maxFaultStart , None)
          }
        }
      } else {
        // 物理值未超过阈值
        (false, None, None)
      }
    } else {
      // 硬故障情况下，返回当前状态
      (true, maxFaultStart, None)
    }
  }

  def clearMaxFault(
                     key: String,
                     faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                     iotData: IoTDBReading,
                     rule: DiagnosisRule
                   ): (Boolean, Option[Long], Option[Long]) = {
    val (maxFault, maxFaultStart, maxFaultRecover, _, _, _, _, _, _, _, hard_result) = faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))
    if (maxFaultStart.isDefined) {
      if (maxFaultRecover.isEmpty) {
        //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault recovery started at ${iotData.timestamp}")
        (true, maxFaultStart, Some(iotData.timestamp))
      } else {
        val recoveryTime = (iotData.timestamp - maxFaultRecover.get) / 1000
        if (recoveryTime >= rule.max_val_clr_time) {
          //out.coNllect(s"Clear: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault cleared at ${iotData.timestamp}")
          (false, None, None)
        } else {
          (true, maxFaultStart, maxFaultRecover)
        }
      }
    } else {
      (false, None, None)
    }
  }

  def establishMinFault(
                         key: String,
                         physicalValue: Option[Double],
                         rule: DiagnosisRule,
                         faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                         iotData: IoTDBReading
                       ): (Boolean, Option[Long], Option[Long]) = {

    // 从故障状态映射中获取当前键的状态
    val (_,_,_,minFault, minFaultStart, minFaultRecover, _, _, _, _, hard_result) = faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))
    // 硬故障--无效
    if (!minFault) {
      // 物理值是否大于极大值
      if (physicalValue.exists(_ < rule.min_val_thres)) {
        if (minFaultStart.isEmpty) {
          // 首次检测到故障
          //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault detected at ${iotData.timestamp}")
          (false, Some(iotData.timestamp), None)
        } else {
          // 计算故障持续时间
          val faultDuration = (iotData.timestamp - minFaultStart.get) / 1000
          if (faultDuration >= rule.min_val_estab_time) {
            // 故障确认
            //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault confirmed at ${iotData.timestamp}")
            (true, minFaultStart, None)
          }else{
            // 仍在怀疑阶段
            (false, minFaultStart, None)
          }
        }
      } else {
        // 物理值未超过阈值
        (false, None, None)
      }
    } else {
      // 硬故障情况下，返回当前状态
      (true, minFaultStart, None)
    }
  }

  def clearMinFault(
                     key: String,
                     faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                     iotData: IoTDBReading,
                     rule: DiagnosisRule
                   ): (Boolean, Option[Long], Option[Long]) = {
    val (_,_,_,minFault, minFaultStart, minFaultRecover, _, _, _, _, hard_result) = faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))
    if (minFaultStart.isDefined) {
      if (minFaultRecover.isEmpty) {
        //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault recovery started at ${iotData.timestamp}")
        (true, minFaultStart, Some(iotData.timestamp))
      } else {
        val recoveryTime = (iotData.timestamp - minFaultRecover.get) / 1000
        if (recoveryTime >= rule.min_val_clr_time) {
          //out.collect(s"Clear: Sensor ${rule.iot_tbl}.${rule.iot_fld} max value fault cleared at ${iotData.timestamp}")
          (false, None, None)
        } else {
          (true, minFaultStart, minFaultRecover)
        }
      }
    } else {
      (false, None, None)
    }
  }

  def establishRateFault(
                          key: String,
                          physicalValue: Option[Double],
                          lastPhysicalValue: Map[String, (Option[Double], Option[Long], Option[Double])],
                          rule: DiagnosisRule,
                          faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                          iotData: IoTDBReading
                        ): (Boolean, Option[Long], Option[Long]) = {

    // 从故障状态映射中获取当前键的状态
    val (maxFault, maxFaultStart, maxFaultRecover, minFault, minFaultStart, minFaultRecover, rateFault, rateFaultStart, rateFaultRecover, deadValueFault, hard_result) =
      faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))

    // 安全地获取上一个物理值
    val lastValueOpt = lastPhysicalValue.get(key).flatMap(_._3)

    // 计算变化率，确保在计算前物理值和当前值都存在
    val rateOpt = for {
      lastValue <- lastValueOpt
      currentValue <- physicalValue
    } yield (currentValue - lastValue).abs

    // 判断是否存在硬故障
    if (hard_result) {
      // 硬故障情况下，返回当前状态
      (rateFault, rateFaultStart, rateFaultRecover)
    } else {
      // 检查变化率是否超过阈值
      rateOpt match {
        case Some(rate) if rate >= rule.rate_chg_thres =>
          if (rateFaultStart.isEmpty) {
            //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} rate fault detected at ${iotData.timestamp}")
            (false, Some(iotData.timestamp), None)
          } else {
            val faultDuration = (iotData.timestamp - rateFaultStart.get) / 1000
            if (faultDuration >= rule.rate_chg_estab_time) {
              //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} rate fault confirmed at ${iotData.timestamp}")
              (true, rateFaultStart, None)
            } else {
              (false, rateFaultStart, None)
            }
          }
        case _ =>
          (false, None, None)
      }
    }
  }

  def clearRateFault(
                      key: String,
                      physicalValue: Option[Double],
                      lastPhysicalValue: Map[String, (Option[Double], Option[Long], Option[Double])],
                      rule: DiagnosisRule,
                      faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                      iotData: IoTDBReading
                    ): (Boolean, Option[Long], Option[Long]) = {
    // 从故障状态映射中获取当前键的状态
    val (maxFault, maxFaultStart, maxFaultRecover, minFault, minFaultStart, minFaultRecover, rateFault, rateFaultStart, rateFaultRecover, deadValueFault, hard_result) =
      faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))

    // 安全地获取上一个物理值
    val lastValueOpt = lastPhysicalValue.get(key).flatMap(_._3)

    // 计算变化率，确保在计算前物理值和当前值都存在
    val rateOpt = for {
      lastValue <- lastValueOpt
      currentValue <- physicalValue
    } yield (currentValue - lastValue).abs
    // 清除条件
    if(rateOpt.get<=rule.rate_chg_thres){
      if (rateFaultStart.isDefined && rateFaultRecover.isEmpty) {
        //out.collect(s"Alert: Sensor ${rule.iot_tbl}.${rule.iot_fld} rate fault recovery started at ${iotData.timestamp}")
        (true, rateFaultStart, Some(iotData.timestamp))
      } else if (rateFaultRecover.isDefined) {
        val recoveryTime = (iotData.timestamp - rateFaultRecover.get) / 1000
        if (recoveryTime >= rule.rate_chg_clr_time) {
          //out.collect(s"Clear: Sensor ${rule.iot_tbl}.${rule.iot_fld} rate fault cleared at ${iotData.timestamp}")
          (false, None, None)
        } else {
          (true, rateFaultStart, rateFaultRecover)
        }
      } else {
        (false, None, None)
      }
    }else{
      (true,rateFaultStart,None)
    }
  }

  //死值建立
  def diagnoseDeadValueFault(
                              key: String,
                              physicalValue: Option[Double],
                              rule: DiagnosisRule,
                              dedValidValues: Map[String, List[(Option[Double], Some[Long])]],
                              faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                              iotData: IoTDBReading
                            ): (Boolean,Map[String, List[(Option[Double], Some[Long])]]) = {

    // 从故障状态映射中获取当前键的状态
    val (_, _, _, _, _, _, _, _, _, dead_bo, hard_result) =
      faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))

    val dedValidValue =updateDedValidValues(key, physicalValue, iotData.timestamp, rule, dedValidValues)

    //死值判定更新之后的值：

    if (dead_bo) return (true,dedValidValues)


    var dead_result=false
    physicalValue match {
      case Some(value) =>
        val currentTime = iotData.timestamp

        val lastCheckTimePoint = dedValidValues.get(key) match {
          case Some(values) if values.nonEmpty => Some(values.map(_._2.get).min)
          case _ => None
        }
        val timeSinceLastCheck:Long = lastCheckTimePoint.map(lastTime => (currentTime - lastTime) / 1000).getOrElse(0)

        // 更新最近的有效值
        val updatedLastValidValues: List[(Option[Double], Some[Long])] = dedValidValue.getOrElse(key, Nil)


        if (timeSinceLastCheck >= rule.dead_zone_thres_z1) {
          // 获取窗口数据
          val windowData = updatedLastValidValues.map(_._1).flatten.toList.sorted


          if (windowData.nonEmpty) {
            val minValue = windowData.head
            val maxValue = windowData.last
            val median = windowData(windowData.length / 2)
            val secondMax = if (windowData.length > 1) windowData.dropRight(1).last else minValue
            val secondMin = if (windowData.length > 1) windowData(1) else minValue

            val condition1 = math.abs(maxValue - minValue) <= rule.dead_zone_thres_v1
            val condition2 = math.abs(median - secondMax) <= rule.dead_zone_thres_v2
            val condition3 = math.abs(median - secondMin) <= rule.dead_zone_thres_v3

            dead_result=condition1 || (condition2 && condition3)
            (dead_result,dedValidValue)
          } else {
//            println(s"打印死值的布尔值：$dead_bo  ${dead_result}")
            (false,dedValidValue)
          }
        } else {
          (false,dedValidValue)
        }
      case None => (false,dedValidValue)
    }
  }

  // 定义清除死值故障的函数
  def clearDeadValueFault(
                           key: String,
                           physicalValue: Option[Double],
                           rule: DiagnosisRule,
                           dedValidValues: Map[String, List[(Option[Double], Some[Long])]],
                           faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                           iotData: IoTDBReading
                         ): (Boolean,Map[String, List[(Option[Double], Some[Long])]]) = {


    // 从故障状态映射中获取当前键的状态
    val (_, _, _, _, _, _, _, _, _, dead_bo, hard_result) =
      faultStatesMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))

    val dedValidValue = updateDedValidValues(key, physicalValue, iotData.timestamp, rule, dedValidValues)
    physicalValue match {
      case Some(value) =>
        val currentTime = iotData.timestamp
        val lastCheckTimePoint = dedValidValues.get(key) match {
          case Some(values) if values.nonEmpty => Some(values.map(_._2.get).min)
          case _ => None
        }
        val timeSinceLastCheck:Long = lastCheckTimePoint.map(lastTime => (currentTime - lastTime) / 1000).getOrElse(0)

        // 更新最近的有效值
        val updatedLastValidValues: List[(Option[Double], Some[Long])] = dedValidValue.getOrElse(key, Nil)

        if (timeSinceLastCheck >= rule.dead_zone_thres_z1) {
          // 获取窗口数据
          val windowData = updatedLastValidValues.map(_._1).flatten.toList.sorted

          if (windowData.nonEmpty) {
            val minValue = windowData.head
            val maxValue = windowData.last
            val median = windowData(windowData.length / 2)
            val secondMax = if (windowData.length > 1) windowData.dropRight(1).last else minValue
            val secondMin = if (windowData.length > 1) windowData(1) else minValue

            val c1 = math.abs(maxValue - minValue) > rule.dead_zone_thres_v1
            val c2 = math.abs(median - secondMax) > rule.dead_zone_thres_v2
            val c3 = math.abs(median - secondMin) > rule.dead_zone_thres_v3

            // 清除条件
//            println(s"故障判定时间33：${(!c1 && (!c2 || !c3),dedValidValue)}")
            (!c1 && (!c2 || !c3),dedValidValue)
          } else {
//            println(s"故障值清楚时间22:${iotData.timestamp}  ${timeSinceLastCheck}")
            (false,dedValidValue)
          }
        }else if(dead_bo){
          (true,dedValidValue)
        } else {
//          println("死值故障清楚时间最外围1111")
          (false,dedValidValue)
        }
      case None => (false,dedValidValue)
    }
  }

  def updateDedValidValues(
                            key: String,
                            physicalValue: Option[Double],
                            timestamp: Long,
                            rule: DiagnosisRule,
                            dedValidValues: Map[String, List[(Option[Double], Some[Long])]]
                          ): Map[String, List[(Option[Double], Some[Long])]] = {

    var updatededValues03 = dedValidValues
    val valuesForKey: Option[List[(Option[Double], Some[Long])]] = dedValidValues.get(key)
    val minTimestamp: Option[Long] = valuesForKey match {
      case Some(values) if values.nonEmpty => Some(values.map(_._2.get).min)
      case _ => None
    }

    if (minTimestamp.isDefined) {
      val timeDiff = (timestamp - minTimestamp.get) / 1000
      if (timeDiff < rule.dead_zone_thres_z1) {
        // 无需进行业务的判断，直接添加新值
        val updatedValuesForKey: List[(Option[Double], Some[Long])] = valuesForKey match {
          case Some(values) if values.nonEmpty =>
            // 去重并添加新值
            val newValues = if (values.exists(_._2.get == timestamp)) {
              values
            } else {
              (physicalValue, Some(timestamp)) :: values
            }
            // 按时间戳排序
            newValues.sortBy(_._2.get)
          case _ => List((physicalValue, Some(timestamp)))
        }

        // 只更新目标键的数据
        updatededValues03 = dedValidValues.updated(key, updatedValuesForKey)
//        println(s"${Thread.currentThread().getName} 小于死值跨域的添加之后的数据：$updatededValues03 时间：$timestamp key:$key 未更新的map:${dedValidValues}")
      } else if (timeDiff > rule.dead_zone_thres_z1) {
        // 去掉最小值，添加新值
        val updatedValuesForKey: List[(Option[Double], Some[Long])] = valuesForKey match {
          case Some(values) if values.nonEmpty =>
            val minTsValue = values.find(_._2.get == minTimestamp.get).get
            // 去掉最小时间戳对应的值
            val filteredValues = values.filterNot(_ == minTsValue)
            // 去重并添加新值
            val newValues = if (filteredValues.exists(_._2.get == timestamp)) {
              filteredValues
            } else {
              (physicalValue, Some(timestamp)) :: filteredValues
            }
            // 按时间戳排序
            newValues.sortBy(_._2.get)
          case _ => List((physicalValue, Some(timestamp)))
        }

        // 只更新目标键的数据
        updatededValues03 = dedValidValues.updated(key, updatedValuesForKey)
//        println(s"${Thread.currentThread().getName} 大于死值跨域的添加之后的数据：$updatededValues03 时间：$timestamp key:$key timdff:$timeDiff 最小时间：${minTimestamp}")
      }
      updatededValues03
    } else {
      // 如果没有最小时间戳，直接添加新值
      val updatedValuesForKey: List[(Option[Double], Some[Long])] = valuesForKey.getOrElse(Nil) :+ (physicalValue, Some(timestamp))
      updatededValues03 = dedValidValues.updated(key, updatedValuesForKey)
//      println(s"${Thread.currentThread().getName} 无最小时间戳的添加之后的数据：$updatededValues03 时间：$timestamp key:$key")
      updatededValues03
    }
  }

  // 合并两个Map
  def mergeMaps(map1: Map[String, List[(Option[Double], Some[Long])]], map2: Map[String, (Option[Double], Some[Long])]): Map[String, List[(Option[Double], Some[Long])]] = {
    map1 ++ map2.map { case (k, v) =>
      k -> (map1.getOrElse(k, Nil) :+ v)
    }
  }

  def toDouble(optAny: Option[Any]): Option[Double] = {
    optAny match {
      case Some(value: Double) => Some(value)
      case Some(value: Float)  => Some(value.toDouble)
      case Some(value: Int)    => Some(value.toDouble)
      case Some(value: Long)   => Some(value.toDouble)
      case Some(value: String) =>
        try {
          Some(value.toDouble)
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }

  def isFalseOrTrue(value: Option[Any]): Boolean = {
    value match {
      case Some("false") => true
      case Some("true")  => true
      case _             => false
    }
  }

}
