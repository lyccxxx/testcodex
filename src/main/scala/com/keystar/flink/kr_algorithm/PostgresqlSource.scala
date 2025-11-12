package com.keystar.flink.kr_algorithm

import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.kr_json.InputData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import scala.collection.mutable.ListBuffer
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import play.api.libs.json.{JsArray, JsValue, Json}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.util.function.Function
import java.util.{List => JavaList}
import javax.script.ScriptEngineManager
import scala.collection.mutable

case class AlgorithmDeviceInstance(
                                    id: Long,
                                    algorithm_id: Long,
                                    productId: Option[Long],
                                    equip_label: String,
                                    algUserEditable: Boolean = false,
                                    algIsRealtime: Boolean = true,
                                    algDurationSeconds: Int = 15,
                                    algParam: Option[String],
                                    alg_input:Option[JsValue],
                                    algFormulaIntermediate: Option[JsValue],
                                    algFormulaFinal: Option[JsValue],
                                    createTime: Timestamp,
                                    updateTime: Timestamp,
                                    siteId: Option[Long],
                                    siteName: Option[String]
                                  )

class PostgresqlSource(sitePartitionMap: mutable.Map[String, Int]) extends RichParallelSourceFunction[String] {

  private var connection: Connection = _
  private var psInitial: PreparedStatement = _
  private var psIncremental: PreparedStatement = _
  private var isRunning: Boolean = true
  private var firstTime: Timestamp = _
  private var numParallelInstances: Int = 0
  private var parallelInstanceId: Int = 0
  private var siteIds:List[String]= _
  private var historicalData: ListBuffer[AlgorithmDeviceInstance] = ListBuffer.empty[AlgorithmDeviceInstance]
  private var lastQueryTime: LocalDateTime = LocalDateTime.now()
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def open(parameters: Configuration): Unit = {
    numParallelInstances = getRuntimeContext.getNumberOfParallelSubtasks
    parallelInstanceId = getRuntimeContext.getIndexOfThisSubtask

    // 指定驱动
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
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
         |SELECT
         |id,
         |algorithm_id,
         |product_id,
         |equip_label,
         |alg_user_editable,
         |alg_is_realtime,
         |alg_duration_seconds,
         |alg_param,
         |alg_input,
         |alg_formula_intermediate,
         |alg_formula_final,
         |create_time,
         |update_time,
         |site_id,
         |site_name
         |FROM public.kr_algorithm_device_instance
         |WHERE alg_is_realtime = true
         |ORDER BY algorithm_id
         |""".stripMargin
    psInitial = connection.prepareStatement(initialQuery)
    psInitial.setFetchSize(1000)

    // 准备增量查询语句
    val incrementalQuery =
      s"""
         |SELECT
         |id,
         |algorithm_id,
         |product_id,
         |equip_label,
         |alg_user_editable,
         |alg_is_realtime,
         |alg_duration_seconds,
         |alg_param,
         |alg_input,
         |alg_formula_intermediate,
         |alg_formula_final,
         |create_time,
         |update_time,
         |site_id,
         |site_name
         |FROM public.kr_algorithm_device_instance
         |WHERE alg_is_realtime = true AND update_time > ?
         |ORDER BY algorithm_id
         |""".stripMargin
    psIncremental = connection.prepareStatement(incrementalQuery)
    psIncremental.setFetchSize(1000)

    siteIds =generatePartitionKeys(sitePartitionMap)
  }

  // 获取最大时间
  private def getMaxTime(): Timestamp = {
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      val sql = "SELECT max(update_time) as upat FROM public.kr_algorithm_device_instance"
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

  override def run(sourceContext: SourceContext[String]): Unit = {
    // 读取初始数据
//    historicalData ++= fetchData(psInitial)
    var num = 0
    while (isRunning) {
      num += 1
//      logger.info(s"每次往下游发送数据：${num}")
      val currentTime = LocalDateTime.now()
      // 计算距离上次查询是否已经过了一天
//      if (ChronoUnit.DAYS.between(lastQueryTime, currentTime) >= 1) {
//        // 检查增量数据
//        psIncremental.setTimestamp(1, firstTime)
//        val incrementalData = fetchData(psIncremental)
//
//        if (incrementalData.nonEmpty) {
//          // 合并增量数据到历史数据
//          historicalData = historicalData.filterNot(oldData => incrementalData.exists(newData => newData.id == oldData.id)) ++ incrementalData
//          // 更新firstTime为最新的update_time
//          incrementalData.foreach { rule =>
//            val updateTime = rule.updateTime
//            if (updateTime.after(firstTime)) {
//              firstTime = updateTime
//            }
//          }
//        }
//        lastQueryTime = currentTime // 更新上次查询时间
//      }
//
//      // 对历史数据按algLabelId字段排序
//      val totalData = historicalData.sortBy(_.algorithm_id)

      // 批量发送数据，使用checkpoint锁保证原子性
      siteIds.distinct.foreach { siteId =>
        sourceContext.collect(siteId)
      }
      // 添加适当的延迟，避免CPU空转
      Thread.sleep(1000)
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

  private def fetchData(statement: PreparedStatement): ListBuffer[AlgorithmDeviceInstance] = {
    var resultSet: ResultSet = null
    try {
      resultSet = statement.executeQuery()
      val rules = ListBuffer[AlgorithmDeviceInstance]()
      while (resultSet.next()) {
        val id = resultSet.getLong("id")
        val rule = AlgorithmDeviceInstance(
          id = resultSet.getLong("id"),
          algorithm_id = resultSet.getLong("algorithm_id"),
          productId = Option(resultSet.getLong("product_id")).filter(_ != 0), // 处理NULL
          equip_label = resultSet.getString("equip_label"),
          algUserEditable = resultSet.getBoolean("alg_user_editable"),
          algIsRealtime = resultSet.getBoolean("alg_is_realtime"),
          algDurationSeconds = resultSet.getInt("alg_duration_seconds"),
          algParam = Option(resultSet.getString("alg_param")),
          alg_input=Option(resultSet.getString("alg_input")).map(Json.parse),
          algFormulaIntermediate = Option(resultSet.getString("alg_formula_intermediate")).map(Json.parse),
          algFormulaFinal = Option(resultSet.getString("alg_formula_final")).map(Json.parse),
          createTime = resultSet.getTimestamp("create_time"),
          updateTime = resultSet.getTimestamp("update_time"),
          siteId = Option(resultSet.getLong("site_id")).filter(_ != 0),
          siteName = Option(resultSet.getString("site_name"))
        )
        rules += rule
      }
//      // ========== 新增：同一设备下position重复校验 ==========
//      // 按设备分组（equip_label）
//      val rulesByEquip = rules.groupBy(_.equip_label)
//
//      rulesByEquip.foreach { case (equipLabel, equipRules) =>
//        // 收集该设备下所有规则的position
//        val allPositions = equipRules.flatMap { rule =>
//          rule.algFormulaFinal.flatMap { formulaFinal =>
//            (formulaFinal \ "formula_final").asOpt[JsArray].toList.flatMap { jsArray =>
//              jsArray.value.flatMap { item =>
//                (item \ "position").asOpt[String].filter(_.nonEmpty)
//              }
//            }
//          }
//        }

//        // 检查是否有重复的position
//        val positionCounts = allPositions.groupBy(identity).mapValues(_.size)
//        val duplicatePositions = positionCounts.filter { case (_, count) => count > 1 }.keys
//
//        if (duplicatePositions.nonEmpty) {
//        //  logger.error(s"设备[${equipLabel}]存在重复的position配置：${duplicatePositions.mkString(", ")}")
//          println(s"设备[${equipLabel}]存在重复的position配置：${duplicatePositions.mkString(", ")}")
//          // 可选：抛出异常中断任务，或仅日志警告
//          // throw new RuntimeException(s"设备[${equipLabel}]存在重复position：${duplicatePositions.mkString(", ")}")
//        }
//      }
//      // ========== 校验结束 ==========

      rules
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
        logger.error("关闭资源时出错", e)
    }
  }
}