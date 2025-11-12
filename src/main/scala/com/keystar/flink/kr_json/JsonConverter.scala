package com.keystar.flink.kr_json

import play.api.libs.json._
import scala.util.matching.Regex

object JsonConverter {
  // 主转换方法
  def convertToJson(jsonString: String): String = {
    try {
      val json = Json.parse(jsonString)

      val algParamJson = parseAlgParam((json \ "alg_param").getOrElse(JsNull))
      val algShowGroupJson = parseAlgShowGroup((json \ "alg_show_group").getOrElse(JsNull))
      val algShowLevelcursorJson = parseAlgShowLevelcursor((json \ "alg_show_levelcursor").getOrElse(JsNull))

      val updatedJson = json.as[JsObject] ++ Json.obj(
        "alg_param" -> algParamJson,
        "alg_show_group" -> algShowGroupJson,
        "alg_show_levelcursor" -> algShowLevelcursorJson
      )

      Json.stringify(updatedJson)
    } catch {
      case e: Exception =>
        println(s"Error converting JSON: ${e.getMessage}")
        jsonString
    }
  }

  // 解析 alg_param 字段 - 改进版本
  private def parseAlgParam(field: JsValue): JsValue = {
    field match {
      case JsString(str) =>
        // 处理字符串格式的 Map(...)
        val mapPattern = """Map\((.*)\)""".r
        str match {
          case mapPattern(content) =>
            // 使用更健壮的解析器处理键值对
            val entries = splitTopLevelCommas(content).flatMap { entry =>
              val keyValuePattern = """([^->]+)\s*->\s*List\(([^)]+)\)""".r
              entry match {
                case keyValuePattern(key, valueStr) =>
                  // 解析 List 中的值，处理嵌套括号
                  val values = splitTopLevelCommas(valueStr).map { v =>
                    try JsNumber(v.trim.toDouble) catch { case _: Exception => JsString(v.trim) }
                  }
                  Some(key.trim -> JsArray(values))
                case _ => None
              }
            }
            JsObject(entries.toMap)
          case _ =>
            // 尝试解析为普通 JSON 对象
            try Json.parse(str).as[JsObject] catch {
              case _: Exception => JsString(str)
            }
        }
      case obj: JsObject => obj
      case _ => JsObject.empty
    }
  }

  // 解析 alg_show_group 字段
  private def parseAlgShowGroup(field: JsValue): JsValue = {
    field match {
      case JsString(str) =>
        val listPattern = """List\((.*)\)""".r
        str match {
          case listPattern(content) =>
            // 处理 Group(...) 对象
            val groups = splitTopLevelCommas(content).flatMap { groupStr =>
              val groupPattern = """Group\(([^,]+),\s*List\(([^)]+)\)\)""".r
              groupStr match {
                case groupPattern(title, lineStr) =>
                  val lines = splitTopLevelCommas(lineStr).map(JsString(_))
                  Some(Json.obj(
                    "group_title" -> title.trim,
                    "group_line" -> JsArray(lines)
                  ))
                case _ => None
              }
            }
            JsArray(groups)
          case _ =>
            try Json.parse(str).as[JsArray] catch {
              case _: Exception => JsString(str)
            }
        }
      case arr: JsArray => arr
      case _ => JsArray.empty
    }
  }

  // 解析 alg_show_levelcursor 字段
  private def parseAlgShowLevelcursor(field: JsValue): JsValue = {
    field match {
      case JsString(str) =>
        val listPattern = """List\((.*)\)""".r
        str match {
          case listPattern(content) =>
            val values = splitTopLevelCommas(content).map(JsString(_))
            JsArray(values)
          case _ =>
            try Json.parse(str).as[JsArray] catch {
              case _: Exception => JsString(str)
            }
        }
      case arr: JsArray => arr
      case _ => JsArray.empty
    }
  }

  // 辅助方法：分割顶级逗号（忽略括号内的逗号）
  private def splitTopLevelCommas(str: String): List[String] = {
    var result = List.empty[String]
    var current = new StringBuilder
    var depth = 0

    str.foreach { c =>
      c match {
        case '(' =>
          depth += 1
          current.append(c)
        case ')' =>
          depth -= 1
          current.append(c)
        case ',' if depth == 0 =>
          result = result :+ current.toString.trim
          current.clear()
        case _ =>
          current.append(c)
      }
    }

    if (current.nonEmpty) {
      result = result :+ current.toString.trim
    }

    result
  }
}