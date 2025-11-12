package Test

import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.kr_json.InputData

import javax.script.ScriptEngineManager

object Test {
  def main(args: Array[String]): Unit = {

    val sourcepath="output.json"
    val jsonStr =scala.io.Source.fromFile(sourcepath)
    //怎么将进行替换
    val responses = List(
      IoTDBReading(1711950465000L, Map("root.ln.`1269014857442188595`.B02_YC0025" -> Some(67.59))),
      IoTDBReading(1711950465000L, Map("root.ln.`1269014857442188595`.B02_YC0024" -> Some(57.59))),
      IoTDBReading(1711950470000L, Map("root.ln.`1269014857442188595`.B02_YC0024" -> Some(55.59))),
      IoTDBReading(1711950470000L, Map("root.ln.`1269014857442188595`.B02_YC0025" -> Some(65.59)))
    )

    val inputs = List(
      InputData("attr_B02_YC0025_1269014857442188595", "iotdb", "root.ln.`1269014857442188595`", "01", "B02_YC0025", "B02_YC0025", "单位", "0.1", "float","10"),
      InputData("attr_B02_YC0024_1269014857442188595", "iotdb", "root.ln.`1269014857442188595`", "01", "B02_YC0024", "B02_YC0024", "单位", "0.1", "float","10")
    )

    val expressions = Seq("/attr_B02_YC0025_1269014857442188595*/attr_B02_YC0024_1269014857442188595>1000", "/attr_B02_YC0025_1269014857442188595==/attr_B02_YC0024_1269014857442188595"
      ,"if (/attr_B02_YC0025_1269014857442188595 > 10 && /attr_B02_YC0024_1269014857442188595 < 5) {1} \nelse if (/attr_B02_YC0025_1269014857442188595 <= 10 && /attr_B02_YC0024_1269014857442188595 >= 5) {2} \nelse {3}",
      "if (/attr_B02_YC0025_1269014857442188595 > 10 && /attr_B02_YC0024_1269014857442188595 < 5) {\"nomvar\"} \nelse if (/attr_B02_YC0025_1269014857442188595 <= 10 && /attr_B02_YC0024_1269014857442188595 >= 5) {\"严重\"} \nelse {\"警告\"}")

    val results = processResponses(responses, inputs, expressions)
    results.foreach(println)
  }

  // 辅助方法：转换值
  private def convertValue(value: Option[Any], valueType: String, accuracy: String): Option[Any] = {
    value.flatMap {
      case num: Double => Some(num.toString)
      case num: Int => Some(num.toString)
      case _ => None
    }
  }

  // 辅助方法：检查表达式中引用的键是否有空值
  private def hasNullKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): Boolean = {
    inputMap.keys.filter(expr.contains).exists(key => inputMap(key).isEmpty)
  }

  // 辅助方法：替换表达式中的键
  private def replaceKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): String = {
    inputMap.keys.filter(expr.contains).foldLeft(expr) { (e, key) =>
      inputMap(key) match {
        case Some(value) => e.replace(key, value.toString)
        case None => e
      }
    }
  }


  def processResponses(
                        responses: List[IoTDBReading],
                        inputs: Iterable[InputData],
                        expressions: Seq[String]
                      ): Seq[(String, String, Option[Any], Long)] = {

    val engine = new ScriptEngineManager().getEngineByName("JavaScript")

    // 按时间戳分组
    val groupedResponses = responses.groupBy(_.timestamp)

    groupedResponses.flatMap { case (timestamp, readings) =>
      // 合并相同时间戳下的 values
      val mergedValues = readings.flatMap(_.values).toMap

      // 构建输入映射
      val inputMap = inputs.map { data =>
        val attr = "/" + data.attr
        val keyToMatch = s"${data.table}.${data.position}"
        val value = mergedValues.get(keyToMatch).flatMap(identity)
        val convertedValue = convertValue(value, data.`type`, data.accuracy)
        attr -> convertedValue
      }.toMap

      expressions.map { expr =>
        if (hasNullKeysInExpr(expr, inputMap)) {
          (expr, expr, None, timestamp) // 如果表达式中存在空键，直接返回 None
        } else {
          val replacedExpr = replaceKeysInExpr(expr, inputMap)
          println(s"正常的表达式为：$replacedExpr")
          try {
            // 动态判断返回值类型
            val result = engine.eval(replacedExpr)
            result match {
              case boolResult if boolResult.isInstanceOf[Boolean] =>
                (expr, replacedExpr, Some(boolResult.asInstanceOf[Boolean]), timestamp)
              case numResult if numResult.isInstanceOf[Double] =>
                (expr, replacedExpr, Some(numResult.asInstanceOf[Double]), timestamp)
              case intResult if intResult.isInstanceOf[Integer] =>
                (expr, replacedExpr, Some(intResult.asInstanceOf[Integer].toDouble), timestamp)
              case strResult if strResult.isInstanceOf[String] =>
                (expr, replacedExpr, Some(strResult.asInstanceOf[String]), timestamp) // 处理 String 类型
              case other =>
                println(s"Unsupported result type for expression '$expr': ${other.getClass}")
                (expr, replacedExpr, None, timestamp) // 不支持的类型返回 None
            }
          } catch {
            case ex: Exception =>
              println(s"Error evaluating expression '$expr' and '$replacedExpr': 异常点：${ex.getMessage}")
              (expr, replacedExpr, None, timestamp) // 捕获异常时返回 None
          }
        }
      }
    }.toSeq
  }
}
