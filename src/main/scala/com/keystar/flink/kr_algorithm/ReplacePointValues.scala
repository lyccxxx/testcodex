package com.keystar.flink.kr_algorithm

import play.api.libs.json._
import scala.util.matching.Regex
case class FormulaJson(output: String, position: String)


object ReplacePointValues extends App {
  implicit val formulaJsonFormat: Format[FormulaJson] = Json.format[FormulaJson]

  import play.api.libs.json._
  import scala.util.matching.Regex

  // 输入：Option[JsValue]（模拟从数据库读取的 JSON 字段）
  // 1. 多个 JSON 数据（模拟批量查询结果）
  val jsonList: List[Option[JsValue]] = List(
    // 第一个 JSON：含单个点位
    Some(Json.parse("""
    {
      "output": "if(iotdb[\"4303_16431\"]<20){0}else{1}",
      "position": "A1_02"
    }
  """)),
    // 第二个 JSON：含多个点位
    Some(Json.parse("""
    {
      "output": "if(iotdb[\"4303_16432\"]>=50 && iotdb[\"4303_16433\"]<=100){1}else{0}",
      "position": "B2_03"
    }
  """)),
    // 第三个 JSON：空（模拟数据库 NULL 值）
    None,
    // 第四个 JSON：不含点位（用于测试过滤）
    Some(Json.parse("""
    {
      "output": "if(100>50){1}else{0}",
      "position": "C3_04"
    }
  """))
  )

  // 正则：匹配 iotdb["xxx"] 中的点位（修正转义符）
  val pattern: Regex = """iotdb\["([^"]+)"\]""".r

  // 提取所有点位 ID（处理 Option 和 JSON 解析）
  val pointIds: List[String] = jsonList.flatten.flatMap { json =>
    (json \ "output").asOpt[String].toList.flatMap { outputStr =>
      pattern.findAllMatchIn(outputStr).map(_.group(1)).toList
    }
  }

  println("提取到的点位ID: " + pointIds.mkString(","))  // 输出：提取到的点位ID: List(4303_16431)

  // 查询数据库构建替换 Map
//  val replacements = pointIds.map(id => id -> getFromDatabase(id)).toMap

  // 替换函数
//  def replaceFunc(m: Match): String = {
//    val pointId = m.group(1)
//    replacements.get(pointId) match {
//      case Some(value) => value.toString
//      case None => "0" // 如果找不到，默认为 0
//    }
//  }

  // 执行替换
//  val output = pattern.replaceAllIn(input, replaceFunc)

  // 输出结果
//  println("替换后的表达式: " + output)

}