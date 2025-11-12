package com.keystar.flink.flink_stream

import org.codehaus.janino.ExpressionEvaluator
import org.json4s._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import play.api.libs.json._

import javax.script.ScriptEngineManager

// 日志记录器
import org.slf4j.LoggerFactory

object PointRelationshipProcessor {
  def main(args: Array[String]): Unit = {

    // 示例数据
    val data = Seq(
      Map(
        "id" -> 553030L,
        "site_id" -> "1821715907433463808",
        "iot_tbl" -> "root.ln.`1821715907433463808",
        "iot_fld" -> "I046_1_YX0245",
        "src_disconn" -> true,
        "samp_freq_mismatch" -> true,
        "samp_freq_diag_time" -> 0,
        "samp_freq_clr_time" -> 60,
        "conv_amp_factor_sign" -> true,
        "conv_amp_factor" -> 1,
        "norm_val_sign" -> true,
        "norm_val" -> 100,
        "auto_mon_max_val" -> true,
        "auto_clr_max_val" -> true,
        "max_val_thres" -> 250,
        "max_val_estab_time" -> 60,
        "max_val_clr_time" -> 60,
        "auto_mon_min_val" -> true,
        "auto_clr_min_val" -> true,
        "min_val_thres" -> 120,
        "min_val_estab_time" -> 60,
        "min_val_clr_time" -> 60,
        "auto_mon_rate_chg" -> true,
        "auto_clr_rate_chg" -> true,
        "rate_chg_thres" -> 50,
        "rate_chg_estab_time" -> 60,
        "rate_chg_clr_time" -> 60,
        "auto_mon_dead_zone" -> true,
        "auto_clr_dead_zone" -> true,
        "dead_zone_thres_z1" -> 120,
        "dead_zone_thres_v1" -> 10,
        "dead_zone_thres_v2" -> 15,
        "dead_zone_thres_v3" -> 20,
        "created_at" -> "11:17.7",
        "updated_at" -> "35:45.4",
        "storage_type" -> "BOOLEAN",
        "alarm" -> true,
        "info_wt_state_json" -> "{}"
      ),
      Map(
        "id" -> 553031L,
        "site_id" -> "1821715907433463808",
        "iot_tbl" -> "root.ln.`1821715907433463808",
        "iot_fld" -> "I046_1_YX0246",
        "src_disconn" -> true,
        "samp_freq_mismatch" -> true,
        "samp_freq_diag_time" -> 0,
        "samp_freq_clr_time" -> 60,
        "conv_amp_factor_sign" -> true,
        "conv_amp_factor" -> 1,
        "norm_val_sign" -> true,
        "norm_val" -> 100,
        "auto_mon_max_val" -> true,
        "auto_clr_max_val" -> true,
        "max_val_thres" -> 250,
        "max_val_estab_time" -> 60,
        "max_val_clr_time" -> 60,
        "auto_mon_min_val" -> true,
        "auto_clr_min_val" -> true,
        "min_val_thres" -> 120,
        "min_val_estab_time" -> 60,
        "min_val_clr_time" -> 60,
        "auto_mon_rate_chg" -> true,
        "auto_clr_rate_chg" -> true,
        "rate_chg_thres" -> 50,
        "rate_chg_estab_time" -> 60,
        "rate_chg_clr_time" -> 60,
        "auto_mon_dead_zone" -> true,
        "auto_clr_dead_zone" -> true,
        "dead_zone_thres_z1" -> 120,
        "dead_zone_thres_v1" -> 10,
        "dead_zone_thres_v2" -> 15,
        "dead_zone_thres_v3" -> 20,
        "created_at" -> "11:17.7",
        "updated_at" -> "35:45.4",
        "storage_type" -> "BOOLEAN",
        "alarm" -> true,
        "info_wt_state_json" -> "{\n  \"rule\": {\n    \"type\": \"(规则类型，用于上报至平台分类)\",\n    \"name\": \"(规则名称，用于上报至平台显示)\",\n    \"desc\": \"(规则描述, 用于描述上报平台时数据解读中定义部分)\",\n    \"alarm\": true,\n    \"level\": \"warning\",\n    \"expression\":[\n      \"/attr1>/attr2\",\n      \"/attr1==/attr2\",\n      \"/attr1</attr2\"\n    ],\n    \"input_data\": [\n      {\n        \"attr\": \"attr1\",\n        \"source\": \"（数据源）iotdb\",\n        \"table\": \"(表名)\",\n        \"template\": \"(数据模版名)\",\n        \"position\": \"I046_1_YX0246\",\n        \"properties\": \"（属性）I046_1_YX0246\",\n        \"unit\": \"(单位)\",\n        \"accuracy\":\"0.1\",\n        \"type\": \"float\",\n        \"value\": \"\"\n      },\n      {\n        \"attr\": \"attr2\",\n        \"source\": \"（数据源）iotdb\",\n        \"table\": \"(表名)\",\n        \"template\": \"(数据模版名)\",\n        \"position\": \"I046_1_YX024\",\n        \"properties\": \"（属性）I046_1_YX0243\",\n        \"unit\": \"(单位)\",\n        \"accuracy\":\"0.1\",\n        \"type\": \"float\",\n        \"value\": \"\"\n      }\n    ],\n    \"output_data\": [\n      {\n        \"output\": \"（第一个表达式输出）/attr1>/attr2\",\n        \"source\": \"（数据源）iotdb\",\n        \"table\": \"(表名)\",\n        \"template\": \"(数据模版名)\",\n        \"position\": \"（点位）I046_1_YX0246\",\n        \"properties\": \"（属性）I046_1_YX0246\",\n        \"unit\": \"(单位)\",\n        \"accuracy\":\"0.1\",\n        \"type\": \"float\",\n        \"value\": \"\",\n        \"alarm\": true,\n        \"level\": \"warning\"\n      },\n      {\n        \"output\": \"（第三个表达式输出）/attr1=/attr2\",\n        \"source\": \"（数据源）iotdb\",\n        \"table\": \"(表名)\",\n        \"template\": \"(数据模版名)\",\n        \"position\": \"（点位）I046_1_YX0246\",\n        \"properties\": \"（属性）I046_1_YX0246\",\n        \"unit\": \"(单位)\",\n        \"accuracy\":\"0.1\",\n        \"type\": \"float\",\n        \"value\": \"\",\n        \"alarm\": true,\n        \"level\": \"warning\"\n      },\n      {\n        \"output\": \"（第三个表达式输出）/attr1</attr2\",\n        \"source\": \"（数据源）iotdb\",\n        \"table\": \"(表名)\",\n        \"template\": \"(数据模版名)\",\n        \"position\": \"（点位）I046_1_YX0246\",\n        \"properties\": \"（属性）I046_1_YX0246\",\n        \"unit\": \"(单位)\",\n        \"accuracy\":\"0.1\",\n        \"type\": \"float\",\n        \"value\": \"\",\n        \"alarm\": true,\n        \"level\": \"warning\"\n      }\n    ]\n  }\n}"
      )
    )



    // 提取 info_wt_state_json 字段
    val infoWtStateJsons = data.map(_.getOrElse("info_wt_state_json", "{}"))

    //连接表读取数据json数据
    //从json里边读取点位
    //取最新的点位数据，先看有效值点位是否存在，存在在进行？
    //并行开启计算？

    data.foreach { row =>
      //获取点位
      val row_iot = row.getOrElse("iot_fld","").toString
      //获取点位的字符串
      val jsonStr = row.getOrElse("info_wt_state_json", "{}").toString
      val json = Json.parse(jsonStr)

      // 直接打印整个 JSON 对象（如果需要）
//      println(s"Raw JSON for ${row("iot_fld")}: $json")

      // 将输入数据映射到 Map[String, Double] 方便查找


      val pyvalue=100

      // 检查 JSON 是否为空对象或空数组
      json match {
        case obj: JsObject if obj.fields.isEmpty => println("The JSON object is empty.")
        case arr: JsArray if arr.value.isEmpty => println("The JSON array is empty.")
        case _ =>
          // 获取 rule 对象
          (json \ "rule").asOpt[JsObject] match {
            case Some(rule) =>
              val inputData = (rule \ "input_data").asOpt[List[JsObject]].getOrElse(List())
              val arr_position=inputData.map{data=>
                val iot_fld = (data\"position").as[String]
                iot_fld
              }
              //判断那个数据是否存在状态管理器里边
              //创建一个长度为3的状态管理器用来接收
              println(s"打印点位数组：$arr_position")
              val inputMap = inputData.map { data =>
                val attr = "/"+(data \ "attr").as[String]
                val iot_fld = (data\"position").as[String]
                val valueString = (data \ "value").as[String]
                val value = if (valueString == null || valueString.isEmpty) {
                  pyvalue // 或者其他默认值
                } else valueString.toDouble
                attr->value
              }.toMap
              // 获取表达式列表
              val expression = (rule \ "expression").as[List[String]]
              // 替换表达式中的键为 Map 中的值，并计算结果
              val engine = new ScriptEngineManager().getEngineByName("JavaScript")
              val results = expression.map { expr =>
                val replacedExpr = inputMap.foldLeft(expr) { case (e, (key, value)) =>
                  e.replace(key, value.toString)
                }
                val result = engine.eval(replacedExpr).asInstanceOf[Boolean] // 假设表达式是布尔表达式
                (expr, replacedExpr, result)
              }

              // 打印结果
              results.foreach { case (originalExpr, replacedExpr, result) =>
                println(s"Original Expression: $originalExpr")
                println(s"Replaced Expression: $replacedExpr")
                println(s"Result: $result")
                println("------")
              }
            case None => println("No 'rule' field found in the JSON.")
          }
      }
    }
  }
  def parseJson(a:String): Unit = {
    val json = Json.parse(a)
  }
}
