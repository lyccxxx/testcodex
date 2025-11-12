package com.keystar.flink.kr_algorithm

import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import org.postgresql.util.PSQLException

import javax.script.{ScriptEngineManager, ScriptException}
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import play.api.libs.json._
import play.api.libs.json.JsValue

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import scala.collection.JavaConverters.{asScalaBufferConverter, iterableAsScalaIterableConverter}
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import java.text.DecimalFormat
import java.util.function.Function
import java.util.{List => JavaList}
import javax.script.ScriptEngineManager
import scala.collection.mutable.ListBuffer

object AlgorithFunction {

  def main(args: Array[String]): Unit = {
    // 示例数据
    val responses = List(
      IoTDBReading(1752051600000L, Map(
        "root.ln.`1522125219418180352`.4303_16431" -> Some(43.5),
        "root.ln.`1522125219418180352`.4303_16432" -> Some(43.3),
        "root.ln.`1522125219418180352`.4303_16437" -> Some(53.3)
      ))
    )

    val num_seq =Json.parse(
      """
        |{
        |	"alg_formula_intermediate": [{
        |		"output": "if(iotdb[\"4303_16431\"]>20&&45>0){0}else{1}",
        |		"position": "A1_02",
        |		"type": "double"
        |	}, {
        |		"output": "if(iotdb[\"4303_16432\"]>20&&45>0){0}else{1}",
        |		"position": "A2_02",
        |		"type": "double"
        |	}, {
        |		"output": "if(iotdb[\"4303_16437\"]>20&&45>0){0}else{1}",
        |		"position": "B1_02",
        |		"type": "double"
        |	}]
        |}
        |""".stripMargin)

    // 提取 alg_formula_intermediate 数组（安全处理，避免空值或类型错误）
    val intermediateSeq: Seq[JsValue] = (num_seq \ "alg_formula_intermediate").asOpt[JsArray] match {
      case Some(jsArray) => jsArray.value.toSeq  // 转换为 Seq[JsValue]
      case None =>
        println("Warning: alg_formula_intermediate 字段不存在或不是数组")
        Seq.empty  // 空序列
    }
    println(s"打印数值情况：${intermediateSeq.size}")

    val pattern: Regex = """iotdb\["([^"]+)"\]""".r

    // 从所有 JSON 对象的 "output" 字段中提取点位  可能存在json嵌套json的关系
    val pointIds: Seq[String] = intermediateSeq.flatMap { json =>
      (json \ "output").asOpt[String].toList.flatMap { outputStr =>
        pattern.findAllMatchIn(outputStr).map(_.group(1)).toList
      }
    }

    println(s"打印数值：${pointIds}")

    // 使用 Play JSON 创建 JsValue
    val inputs = Seq(
      Json.parse("""{"output": "if(iotdb[\"4303_16431\"]>20&&45>0){0}else{1}", "position": "A1_02", "type": "double"}"""),
      Json.parse("""{"output": "if(iotdb[\"4303_16432\"]<20){0}else{1}", "position": "A2_02", "type": "double"}""")
    )

    // 执行处理
    val results = processResponses(responses, intermediateSeq)

    println(results)
    // 获取 A1_02 对应的结果
    val a102Result = results
      .find(_._5 == "A1_02")
      .flatMap(_._3)

    val a101Result = results
      .find(_._5 == "A2_02")
      .flatMap(_._3)

    println(s"A1_02 对应的结果: $a102Result") // 输出: Some(1.0)
    println(s"打印a202的值：$a101Result")

    val instance1 = AlgorithmDeviceInstance(
      id = 1,
      algorithm_id = 100,
      productId = None,
      algDurationSeconds = 30, // 持续时间 30 秒
      equip_label="",
      algParam = Some("threshold=80"),
      // 中间表达式：{"output": "...", "position": "A1_01"}
      alg_input=Some(Json.parse("{}")),
      algFormulaIntermediate = Some(Json.parse("""{"output": "if(iotdb[\"4303_16430\"]>=51.2){1}else{0}", "position": "A1_01_zhong"}""")),
      // 最终表达式：{"output": "...", "position": "A1_02"}
      algFormulaFinal = Some(Json.parse("""{"output": "if(iotdb[\"4303_16431\"]<20){0}else{1}", "position": "A1_02"}""")),
      createTime = new Timestamp(System.currentTimeMillis()),
      updateTime = new Timestamp(System.currentTimeMillis()),
      siteId = None,
      siteName = None
    )

    val instance2 = AlgorithmDeviceInstance(
      id = 2,
      algorithm_id = 101,
      productId = None,
      equip_label="",
      algDurationSeconds = 32, // 持续时间 32 秒
      algParam = Some("threshold=82"),
      alg_input=Some(Json.parse("{}")),
      // 中间表达式：{"output": "...", "position": "A2_02"}
      algFormulaIntermediate = Some(Json.parse("""{"output": "if(iotdb[\"4303_16432\"]>=54.2){1}else{0}", "position": "A2_02_zhong"}""")),
      // 最终表达式：{"output": "...", "position": "A2_02"}（注意：position 重复）
      algFormulaFinal = Some(Json.parse("""{"output": "if(iotdb[\"4303_16432\"]<20){0}else{1}", "position": "A2_02"}""")),
      createTime = new Timestamp(System.currentTimeMillis()),
      updateTime = new Timestamp(System.currentTimeMillis()),
      siteId = None,
      siteName = None
    )
    val durationMap = buildPositionDurationMap(List(instance1, instance2))

    // 输出映射结果
    println(s"打印转化的结果：${durationMap}")

    val algInputJson: JsValue = Json.parse("""{
        "input": [
          {
            "attr": "myTemp",
            "source": "iotdb",
            "table": "site_123_iotdb",
            "position": "4311_16386",
            "properties": "用户选的设备A1_01"
          },
          {
            "attr": "myPower",
            "source": "iotdb",
            "table": "site_123_iotdb22",
            "position": "4311_16813",
            "properties": "用户选的设备A1_01"
          }
        ]
      }""")
    val interSeq: Seq[JsValue] = (algInputJson \ "input").asOpt[JsArray] match {
      case Some(jsArray) => jsArray.value.toSeq  // 转换为 Seq[JsValue]
      case None =>
        println("Warning: alg_formula_intermediate 字段不存在或不是数组")
        Seq.empty  // 空序列
    }
    val allTables: Seq[String] = interSeq.flatMap { jsValue =>
      // 安全提取 table 字段（忽略不存在或非字符串的情况）
      (jsValue \ "table").asOpt[String]
    }

    // （可选）去重处理（如果需要唯一的表名集合）
    val uniqueTables = allTables.distinct.mkString(",")
    println(uniqueTables)

    val num = convertValue(Some(28.81),"float")
    println("打印结果值：===============")
    println(num)

  }


  def filterRules(rules: ListBuffer[DiagnosisRule], data: String, iot_tbl: String): ListBuffer[DiagnosisRule] = {
    val parts = iot_tbl.split("_")
    val tableKey = if (parts.length > 1) {
      parts.init.mkString("_") // 所有除最后一个部分外拼接回来
    } else {
      iot_tbl
    }
    val tableSet = iot_tbl.split(",").map(_.trim).filter(_.nonEmpty).toSet
    val dataFields = data.split(",").map(_.trim).toSet
    rules.filter(rule => dataFields.contains(rule.iot_fld) && tableSet.contains(rule.iot_tbl))
  }

  // 定义一个函数来替换键和值
  def replaceKeysAndValues(readings: List[IoTDBReading]): List[IoTDBReading] = {
    readings.map { reading =>
      val updatedValues = reading.values.flatMap { case (oldKey, value) =>
        // 根据旧键生成新键
        val newKeyPrefix = oldKey.split('.').last
        val newKey = s"${oldKey.replace(s".${newKeyPrefix}", "")}.${newKeyPrefix}_valid"

        // 查找是否有对应的默认值
        Map(newKey -> value)
      }.toMap
      reading.copy(values = updatedValues)
    }
  }

  def get_data(iot_tbl: String): ListBuffer[DiagnosisRule] = {
    // 指定驱动
    val driver = "org.postgresql.Driver"
    // 创建连接所需参数 url
    val url = "jdbc:postgresql://172.16.1.34:5432/data"
    val user = "postgres"
    val password = "K0yS@2024"
    var connection: Connection = null
    var psIncremental: PreparedStatement = null
    var incrementalResultSet: ResultSet = null

    try {
      Class.forName(driver)
      // 创建数据库连接
      connection = DriverManager.getConnection(url, user, password)
      connection.setAutoCommit(false)

      // 准备增量查询语句
      val incrementalQuery =
        s"""
           |SELECT *,CASE
           |        WHEN LENGTH(iot_fld) - LENGTH(REPLACE(iot_fld, '_', '')) >= 2 THEN split_part(iot_fld, '_', 1) || '_' || split_part(iot_fld, '_', 2)
           |        ELSE split_part(iot_fld, '_', 1)
           |    END AS iot_dev
           |FROM public.kr_diagnosis_rules
           |order by site_id
           |""".stripMargin

      //      println(s"打印sql:${incrementalQuery}")
      val maxRetries = 3 // 最大重试次数
      var retries = 0
      var success = false
      var rules = scala.collection.mutable.ListBuffer[DiagnosisRule]()

      while (!success && retries < maxRetries) {
        try {
          psIncremental = connection.prepareStatement(incrementalQuery)
          incrementalResultSet = psIncremental.executeQuery()

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
            rules += rule
          }
          success = true
        } catch {
          case e: PSQLException if e.getMessage.contains("An I/O error occurred while sending to the backend.") =>
            retries += 1
            println(s"捕获到 I/O 错误，第 $retries 次重试，等待 1 秒后重新执行查询...")
            Thread.sleep(1000)
          case e: Exception =>
            throw e
        } finally {
          if (incrementalResultSet != null) incrementalResultSet.close()
          if (psIncremental != null) psIncremental.close()
        }
      }
      if (!success) {
        throw new RuntimeException("经过多次重试后，仍然无法成功执行查询。")
      }

      connection.close()
      rules
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def buildPositionDurationMap(instances: List[AlgorithmDeviceInstance]): Map[String, (Int, Boolean,Long,String)] = {
    instances.flatMap { instance =>
        val duration = instance.algDurationSeconds
        val algorithm_model_id=instance.algorithm_id
        val equip_label = instance.equip_label
        // 提取 intermediate 和 final 的 position 和 duration_status
        val intermediatePositions = instance.algFormulaIntermediate
          .map(extractFromArray_formula_intermediate)  // 提取数组中所有元素
          .getOrElse(Seq.empty).map {
          case (pos, status) => pos -> (duration, status,algorithm_model_id,equip_label)
        }
        // 正确处理数组中的所有元素
        val finalPositions = instance.algFormulaFinal // 展平可能的双重Option
          .map(extractFromArray_formula_final)  // 提取数组中所有元素
          .getOrElse(Seq.empty)
          .map { case (pos, status) => pos -> (duration, status,algorithm_model_id,equip_label) }

        intermediatePositions ++ finalPositions
      }
      // 按 position 分组，取每组的第一个元素（假设同一 position 的 duration 和 status 相同）
      .groupBy(_._1)
      .mapValues(_.head._2) // 转换为 Map[String, (Int, Boolean)]
      .toMap
  }

  // 从 JsValue 中提取 position 和 duration_status 字段
  def extractFromArray_formula_final(js: JsValue): Seq[(String, Boolean)] = {
    // 先将JsValue转换为JsArray
    (js \ "formula_final").asOpt[JsArray] match {
      case Some(jsArray) =>
        // 遍历数组中的每个元素提取数据
        jsArray.value.flatMap { item =>
          for {
            pos <- (item \ "position").asOpt[String]
            status <- (item \ "duration_status").asOpt[Boolean]
          } yield (pos, status)
        }
      case None =>
        Seq.empty
    }
  }

  def extractFromArray_formula_intermediate(js: JsValue): Seq[(String, Boolean)] = {
    (js \ "formula_intermediate").asOpt[JsArray] match {
      case Some(jsArray) =>
        // 遍历数组中的每个元素提取数据
        jsArray.value.flatMap { item =>
          for {
            pos <- (item \ "position").asOpt[String]
            status <- (item \ "duration_status").asOpt[Boolean]
          } yield (pos, status)
        }
      case None =>
        Seq.empty
    }
  }

  def processResponses(
                        responses: List[IoTDBReading],    // IoTDBReading 列表（时序数据库查询结果，包含时间戳和对应值）
                        inputs: Seq[JsValue]             // JsValue 序列（JSON 格式的输入配置，包含计算规则等）
                      ): Seq[(String, String, Option[Any], Long, String)] = { // 返回值：元组序列 (原始表达式, 替换后表达式, 计算结果, 时间戳, 位置标识)

    val engine = new ScriptEngineManager().getEngineByName("JavaScript")   //创建 JavaScript 脚本引擎，执行后续的表达式计算

    // 按时间戳分组处理
    val groupedResponses = responses.groupBy(_.timestamp)
    val groupData = groupedResponses.flatMap { case (timestamp, readings) =>
      // 合并相同时间戳下的所有值
      val mergedValues = readings.flatMap(_.values).toMap

      // 处理每个输入的 JsValue
      inputs.flatMap { input =>
        // 使用 Play JSON API 解析字段
        val exprOpt = (input \ "output").asOpt[String]        // 提取计算表达式（如 "iotdb['temp'] > 30"）
        val positionOpt = (input \ "position").asOpt[String]  // 提取位置标识
        val typeOpt = (input \ "type").asOpt[String]           // 提取值类型

        (exprOpt, positionOpt, typeOpt) match {
          // 仅当 expr、position、valueType 都存在时，才继续处理
          case (Some(expr), Some(position), Some(valueType)) =>

            // 1. 提取所有 iotdb['xxx'] 或 kryj['xxx'] 格式的 key
            val pattern: Regex = """(?:iotdb|kryj)\['([^']+)'\]""".r  // 修正正则，匹配[]包裹的内容

            val inputMap = pattern
              .findAllMatchIn(expr)
              .map { m =>
                val metricId = m.group(1)   // 提取指标名:从正则匹配结果 m 中提取第一个分组（group(1)）

                // 从合并值中提取设备ID（基于 key 格式 root.ln.deviceId.metricId）
                // 更可靠地获取deviceId（增加容错和日志）
                val deviceId = mergedValues.headOption match {
                // 从 mergedValues 的第一个键（firstKey）中提取设备 ID
                  case Some((firstKey, _)) =>
                    val parts = firstKey.split("\\.")     // 按 "." 分割 key
                    if (parts.length >= 3) parts(2) else {  // 若数组长度≥3，取索引 2 的元素作为 deviceId
                      println(s"警告：key [$firstKey] 格式不符合预期，无法提取deviceId")  // 否则打印警告并返回空字符串
                      ""
                    }
                  case None =>
                    println("警告：mergedValues为空，无法提取deviceId")   // 若 mergedValues 为空（无指标值），同样打印警告并返回空字符串。
                    ""
                }
                // 构造待匹配的key
                val keyToMatch = s"root.ln.$deviceId.$metricId"

                // 检查key是否存在于mergedValues中
                val value = mergedValues.get(keyToMatch).flatMap {
                  case Some(v) => Some(v)
                  case None =>
                    println(s"警告：key [$keyToMatch] 在mergedValues中不存在")
                    None
                }
                // 转换值并返回映射
                val convertedValue = convertValue(value, valueType)
                (m.matched, convertedValue)
              }
              .toMap

            // 2. 判断是否有缺失的 key
            val hasNull = inputMap.exists(_._2.isEmpty)

            // 若存在缺失值，返回包含空结果的元组 (原始表达式, 原始表达式, 空结果, 时间戳, 位置)。
            if (hasNull) {
              Seq((expr, expr, None, timestamp, position))
            } else {
              // 3. 替换表达式中的 key 为实际值
              val replacedExpr = replaceIotdbKeys(expr, inputMap)

              // 4. 执行替换后的表达式
              try {
                // 注册单调函数
                // 判断序列是否严格递增
                engine.put("isStrictlyIncreasing", new java.util.function.Function[java.util.List[Double], Boolean] {
                  override def apply(arr: java.util.List[Double]): Boolean =
                    isStrictlyIncreasing(arr.asScala.map(_.doubleValue()).toArray)
                })
                // 判断序列是否严格递减
                engine.put("isStrictlyDecreasing", new java.util.function.Function[java.util.List[Double], Boolean] {
                  override def apply(arr: java.util.List[Double]): Boolean =
                    isStrictlyDecreasing(arr.asScala.map(_.doubleValue()).toArray)
                })
                // 判断序列是否严格恒定
                engine.put("isStrictlyConstant", new java.util.function.Function[java.util.List[Double], Boolean] {
                  override def apply(arr: java.util.List[Double]): Boolean =
                    isStrictlyConstant(arr.asScala.map(_.doubleValue()).toArray)
                })

                // 执行表达式:使用 JavaScript 引擎执行替换后的表达式 replacedExp，得到计算结果 result
                val result = engine.eval(replacedExpr)

                // 6. 转换结果类型并返回
                val processedResult = result match {
                  case b: java.lang.Boolean => Some(b.booleanValue()) // 处理布尔值
                  case d: java.lang.Double => Some(d) // 处理双精度浮点数
                  case i: java.lang.Integer => Some(i) // 处理整数（转为 Double）
                  case s: java.lang.String => Some(s) // 处理字符串
                  case _ =>
                    println(s"不支持的结果类型: ${result.getClass}")
                    None
                }
                Seq((expr, replacedExpr, processedResult, timestamp, position))
              } catch {
                case ex: ScriptException =>
                  println(s"表达式执行失败: $expr, 错误: ${ex.getMessage}")
                  Seq((expr, replacedExpr, None, timestamp, position))   // 打印错误信息并返回包含空结果的元组
              }
            }
          case _ =>    // 若模式匹配中字段不完整（case _），返回空序列
            Seq.empty
        }
      }
    }.toSeq

    groupData
  }

  // 辅助函数：严格单调递增
  def isStrictlyIncreasing(arr: Array[Double]): Boolean = {
    // 边界检查：空数组或单元素数组
    if (arr.isEmpty) {
      // 空数组无法判断是否严格递减，返回 false 或抛出异常，视需求而定
      return false
    }
    if (arr.length == 1) {
      // 单元素数组认为是严格递减的
      return true
    }
    arr.zip(arr.tail).forall { case (a, b) => a < b }
  }

  // 辅助函数：严格单调递减
  def isStrictlyDecreasing(arr: Array[Double]): Boolean = {
    // 边界检查：空数组或单元素数组
    if (arr.isEmpty) {
      // 空数组无法判断是否严格递减，返回 false 或抛出异常，视需求而定
      return false
    }
    if (arr.length == 1) {
      // 单元素数组认为是严格递减的
      return true
    }
    arr.zip(arr.tail).forall { case (a, b) => a > b }
  }

  // 辅助函数：严格保持不变（所有元素相同）
  def isStrictlyConstant(arr: Array[Double]): Boolean = {
    // 边界检查：空数组或单元素数组
    if (arr.isEmpty) {
      // 空数组无法判断是否严格递减，返回 false 或抛出异常，视需求而定
      return false
    }
    if (arr.length == 1) {
      // 单元素数组认为是严格递减的
      return true
    }
    arr.zip(arr.tail).forall { case (a, b) => a == b }
  }


  def formatNumber(num: Double, accuracy: String): Option[String] = {
    try {
      val precision = accuracy.toInt
      if (precision < 0) {
        Some(num.toString) // 如果精度为负数，直接返回原始值
      } else {
        val pattern = s"#.${"#" * precision}" // 生成格式化模式
        val formatter = new DecimalFormat(pattern)
        Some(formatter.format(num)) // 格式化并返回
      }
    } catch {
      case _: NumberFormatException => Some(num.toString) // 如果精度无效，返回原始值
    }
  }

  //  判断表达式中是否包含空值的键
  def hasNullKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): Boolean = {
    // 获取 inputMap 中所有的键,过滤出表达式 expr 中实际包含的键,过滤后键值为空返回 true
    inputMap.keys.filter(expr.contains).exists(key => inputMap(key).isEmpty)
  }

  // 提取表达式中所有 iotdb["指标ID"] 中的指标ID
  def extractIotdbKeys(expr: String): List[String] = {
    val pattern: Regex = """(?:iotdb|kryj)$$'([^']+)'$$""".r
    pattern.findAllMatchIn(expr).map(_.group(1)).toList
  }

  // 替换表达式中的 iotdb["指标ID"] 为实际值
  def replaceIotdbKeys(expr: String, inputMap: Map[String, Option[String]]): String = {
    inputMap.foldLeft(expr) { case (currentExpr, (iotdbKey, valueOpt)) =>  //  使用 foldLeft 对 inputMap 进行迭代，初始值为原始表达式 expr，逐步替换其中的引用,currentExpr存储当前替换后的表达式
      valueOpt match {
        case Some(value) =>  // 给替换的值包裹括号，避免负数相减时的语法歧义
          currentExpr.replace(iotdbKey, s"($value)")
        case None => currentExpr
      }
    }
  }

  // 转换值类型（移除精度处理）
  def convertValue(value: Option[Any], valueType: String): Option[String] = {
    value.flatMap {
      // 新增处理 float 类型的分支
      case num: Double if valueType == "float" => Some(num.toFloat.toString)
      case num: Float if valueType == "float" => Some(num.toString)
      case num: Int if valueType == "float" => Some(num.toFloat.toString)
      case num: Long if valueType == "float" => Some(num.toFloat.toString)

      case num: Double if valueType == "double" => Some(num.toString)
      case num: Float if valueType == "double" => Some(num.toDouble.toString)
      case num: Int if valueType == "double" => Some(num.toDouble.toString)
      case num: Long if valueType == "double" => Some(num.toDouble.toString)
      case num: Int if valueType == "int" => Some(num.toString)
      case num: Long if valueType == "int" => Some(num.toInt.toString)
      case num: Double if valueType == "int" => Some(num.toInt.toString)
      case bool: Boolean if valueType == "boolean" => Some(bool.toString)
      case str: String if valueType == "string" => Some(str)
      case _ => None
    }
  }


}
