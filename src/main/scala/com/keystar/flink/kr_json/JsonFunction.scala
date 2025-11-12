package com.keystar.flink.kr_json

import com.keystar.flink.iotdbstream.IoTDBReading
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, sendRequest}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction

import java.text.DecimalFormat
import java.time.Instant
import java.util.function.Function
import java.util.{List => JavaList}
import javax.script.ScriptEngineManager
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.matching.Regex

object JsonFunction {
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
      val inputMap = inputs.flatMap { data =>
        val attr = data.attr
        val keyToMatch = s"${data.table}.${data.position}"
        val value: Option[Any] = mergedValues.get(keyToMatch).flatMap(identity)
        val convertedValue = convertValue(value, data.`type`, data.accuracy)
        convertedValue.map { cv =>
          // 创建包含转换值和时间戳的元组
          attr -> Some((cv, timestamp))
        }
      }.toMap

      // 先提取所有唯一的键（假设values是Map[String, Any]类型）
      val allKeys = inputMap.keys.toSet

      // 筛选包含任意键的expressions
      val num_express = expressions.filter { expr =>
        allKeys.exists(expr.contains)
      }

      val nsize= num_express.size
      val exsize= expressions.size
      val allsize=allKeys.size

      expressions.map { expr =>
        //先判定这个expr表达式是否存在那三个函数的
        //存在函数的话就获取到上一个状态值，当上一个状态值
        val num03 =hasNullKeysInExpr(expr, inputMap)
        if (hasNullKeysInExpr(expr, inputMap)) {
          (expr, expr, None, timestamp) // 如果表达式中存在空键，直接返回 None
        } else {
          val replacedExpr = replaceKeysInExpr(expr, inputMap)
          try {

            // 注册单调递增
            engine.put("isStrictlyIncreasing", new Function[JavaList[Double], Boolean] {
              override def apply(arrObj: JavaList[Double]): Boolean = {
                val arr: Array[Double] = arrObj.asScala.map(_.doubleValue()).toArray
                isStrictlyIncreasing(arr)
              }
            })

            // 注册单调递减
            engine.put("isStrictlyDecreasing", new Function[JavaList[Double], Boolean] {
              override def apply(arrObj: JavaList[Double]): Boolean = {
                val arr: Array[Double] = arrObj.asScala.map(_.doubleValue()).toArray
                isStrictlyDecreasing(arr)
              }
            })

            // 注册恒定不变
            engine.put("isStrictlyConstant", new Function[JavaList[Double], Boolean] {
              override def apply(arrObj: JavaList[Double]): Boolean = {
                val arr: Array[Double] = arrObj.asScala.map(_.doubleValue()).toArray
                isStrictlyConstant(arr)
              }
            })

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

  def convertValue(value: Option[Any], valueType: String, accuracy: String): Option[String] = {
    value.flatMap {
      case num: Double =>
        formatNumber(num, accuracy)

      case num: Float =>
        formatNumber(num.toDouble, accuracy) // 将 Float 转换为 Double 进行格式化

      case num: Int =>
        Some(num.toString) // 整数不需要格式化

      case num: Long =>
        Some(num.toString) // 长整型也不需要格式化

      case str: String =>
        try {
          if (str.contains(".")) {
            // 如果字符串包含小数点，尝试解析为 Double
            val parsedNum = str.toDouble
            formatNumber(parsedNum, accuracy)
          } else {
            // 否则尝试解析为 Int
            val parsedNum = str.toInt
            Some(parsedNum.toString)
          }
        } catch {
          case _: NumberFormatException => None // 如果无法解析为数值，返回 None
        }
      case num:Boolean=> Some(num.toString)

      case _ => None // 不支持的类型返回 None
    }
  }

  def hasNullKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): Boolean = {
    inputMap.keys.filter(expr.contains).exists(key => inputMap(key).isEmpty)
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

  // 提取表达式中的函数调用子表达式
  def extractFunctionCallsFromExpr(expr: String): List[String] = {
    val pattern: Regex = """isStrictly(?:Increasing|Decreasing|Constant)\([^)]+\)""".r
    pattern.findAllMatchIn(expr).map(_.matched).toList.distinct
  }


  def replaceKeysInExpr(expr: String, inputMap: Map[String, Option[Any]]): String = {
    inputMap.flatMap { case (key, optValue) =>
      optValue.collect {
        case (currentValue: Any, currentTimestamp: Long) => (key, currentValue)
      }
    }.foldLeft(expr) { (currentExpr, entry) =>
      val (key, currentValue) = entry
      val escapedKey = java.util.regex.Pattern.quote(key)
      val regex = s"\\b$escapedKey\\b".r

      // 提取包含该key的函数调用
      val functionCalls = extractFunctionCallsFromExpr(currentExpr).filter(_.contains(key))

      if (functionCalls.isEmpty) {
        // 替换独立的key
        regex.replaceAllIn(currentExpr, currentValue.toString)
      } else {
        // 处理函数调用中的key
        functionCalls.foldLeft(currentExpr) { (exprWithReplacedCalls, functionCall) =>
          val functionRegex = java.util.regex.Pattern.quote(functionCall).r
          // 替换函数内部的key
          val replacedFunctionCall = regex.replaceAllIn(functionCall, currentValue.toString)
          functionRegex.replaceAllIn(exprWithReplacedCalls, replacedFunctionCall)
        }
      }
    }
  }




}
