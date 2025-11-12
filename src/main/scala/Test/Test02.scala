package Test

import com.keystar.flink.kr_json.OutResult
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.text.DecimalFormat
import java.time.Instant
import scala.util.matching.Regex

class CustomPartitioner extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = {
    // 这里可以根据具体逻辑实现分区，例如根据组串 ID 的哈希值分区
    key.hashCode() % numPartitions
  }
}

object Test02 {
  def replaceKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): String = {
    inputMap.keys.filter(expr.contains).foldLeft(expr) { (e, key) =>
      inputMap(key) match {
        case Some(value: List[Option[Any]]) =>
          val listStr = value.flatten.map(_.toString).mkString("[", ",", "]")
          e.replace(key, listStr)
        case Some(value: List[_]) =>
          e.replace(key, value.map(_.toString).mkString("[", ",", "]"))
        case Some(value) if value != null =>
          e.replace(key, value.toString)
        case _ =>
          e
      }
    }
  }



  def main(args: Array[String]): Unit = {
    // 假设这是原始的 inputMap 类型
    val inputMap: Map[String, Option[Any]] = Map(
      "infoWTState" -> Some(10),
      "infoValue2" -> Some(List(Some(20), Some(30))),
      "infoValue" -> Some(List(Some(10), Some(10)))
    )
    val replaceExp = "infoWTState > 5 && infoValue2.contains(20) && infoValue.contains(10)"
    // 进行类型转换
    val convertedInputMap = inputMap.map { case (k, v) => k -> Some(v) }
    val replacedExpr03 = replaceKeysInExpr(replaceExp, convertedInputMap)
    println(s"替换后的表达式: $replacedExpr03")

    val expression = "((infoWTState == 10) || (infoValue == 10)) &&(isStrictlyIncreasing(infoValue2))"
    val pattern: Regex = """isStrictlyIncreasing\([^)]+\)""".r
    val matches = pattern.findFirstIn(expression)
    matches.foreach { matchResult =>
      println(s"提取结果: $matchResult")
    }
  }
}


