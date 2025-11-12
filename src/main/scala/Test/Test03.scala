package Test


import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import play.api.libs.json.{Format, Json}

import java.io.File
import java.util.function.{BiFunction, Function}
import java.util.{List => JavaList}
import javax.script.{ScriptEngine, ScriptEngineManager}
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}
import scala.collection.mutable
import scala.util.matching.Regex

case class OutResult(
                      timestamp: Long,
                      fieldName: String,
                      value: Option[Any],
                      alarm: Boolean,
                      path: String
                    )

class RuleTwoProcessor extends KeyedProcessFunction[String, OutResult, OutResult] {

    // 定义 inputMap 和 outputMap（根据你的需求调整）
    private val inputMap = Map(
      "root.ln.`1816341324371066880`.B06_YC0035_datasample" -> "/attr1",
      "root.ln.`1816341324371066880`.B06_YC0036_datasample" -> "/attr2"
    )

    private val outputMap = Map(
      "/attr1 * /attr2 > 1000" -> (
        "B06_YC0037_datasample",
        "root.ln.`1816341324371066880`",
        true
      )
    )

    // 缓存当前 Key 的字段值
    private var variableValues: MapState[String, Any] = _

    override def open(parameters: Configuration): Unit = {
      val descriptor = new MapStateDescriptor[String, Any](
        "variableValues",
        classOf[String],
        classOf[Any]
      )
      variableValues = getRuntimeContext.getMapState(descriptor)
    }

    override def processElement(
                                 value: OutResult,
                                 ctx: KeyedProcessFunction[String, OutResult, OutResult]#Context,
                                 out: Collector[OutResult]
                               ): Unit = {
      // 提取变量名（根据路径映射）
      val variableName = inputMap.get(value.path).getOrElse {
        throw new IllegalArgumentException(s"Unknown path: ${value.path}")
      }

      // 存储字段值到状态中
      value.value.foreach { v =>
        variableValues.put(variableName, v)
      }

      // 检查是否所有变量已收集
      val requiredVariables = inputMap.values.toSet
      val currentVariables = variableValues.keys().iterator().asScala.toSet
      if (currentVariables == requiredVariables) {
        // 触发计算并清空缓存
        computeAndEmit(value.timestamp, out)
        variableValues.clear()
      }
    }

    // 使用 JavaScript 计算表达式
    private def computeAndEmit(
                                timestamp: Long,
                                out: Collector[OutResult]
                              ): Unit = {
      val engine = new ScriptEngineManager().getEngineByName("JavaScript")
    }

  }

import javax.script.ScriptEngineManager
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object Test03 {

  // 辅助函数：严格单调递增

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
  val exr ="((infoWTState == 1) || (infoWTState == 2))&&(infoWTFault>0)&&((windSpeed>=1.5*2.5)&&(windSpeed<=24))"
  val inputMap=Map("infoWTState"->Some(Some(false),12000L),"infoWTFault"->Some(Some(0.0),12000L),"windSpeed"->Some(Some(0.0),12000L))
  // 提取表达式中的函数调用子表达式
  def extractFunctionCallsFromExpr(expr: String): List[String] = {
    val pattern: Regex = """isStrictly(?:Increasing|Decreasing|Constant)\([^)]+\)""".r
    pattern.findAllMatchIn(expr).map(_.matched).toList.distinct
  }

  import scala.collection.mutable

  def replaceKeysInExpr(expr: String, inputMap: Map[String, Option[Any]], numMap: mutable.Map[String, List[(Any, Long)]]): String = {
    inputMap.keys.filter(expr.contains).foldLeft(expr) { (e, key) =>
      inputMap(key) match {
        case Some((Some(currentValue: Any), currentTimestamp: Long)) =>
          // 提取包含当前键的函数调用
          val functionCalls = extractFunctionCallsFromExpr(e).filter(_.contains(key))
          if(functionCalls.isEmpty){
            e.replace(key, currentValue.toString)
          }else{
            // 替换函数调用中的参数
            val newExpr = functionCalls.foldLeft(e) { (innerExpr, functionCall) =>
              // 获取历史值
              val historyValues = numMap.getOrElse(functionCall, List.empty[(Any, Long)])
                .filter { case (_, timestamp) => Math.abs(currentTimestamp - timestamp) == 15000 } // 时间差为 15 秒
                .map(_._1)

              // 组合历史值和当前值
              val allValues = historyValues :+ currentValue

              // 将函数调用中的参数替换为实际值列表
              val paramStart = functionCall.indexOf('(') + 1
              val paramEnd = functionCall.indexOf(')')
              val param = functionCall.substring(paramStart, paramEnd)

              innerExpr.replace(param, allValues.mkString("[", ",", "]"))
            }

            // 更新 numMap
            functionCalls.foreach { functionCall =>
              if (!numMap.contains(functionCall)) {
                // 首次出现，将 currentValue 和 currentTimestamp 存入 numMap
                numMap.put(functionCall, List((currentValue, currentTimestamp)))
              } else {
                // 不是首次出现，更新 numMap 中的值
                val updatedValues = List((currentValue, currentTimestamp))
                numMap.remove(functionCall)
                numMap.put(functionCall, updatedValues)
              }
            }
            newExpr
          }
        case _ => e
      }
    }
  }


  def main(args: Array[String]): Unit = {
    // 使用 JavaScript 引擎执行表达式
    val engine = new ScriptEngineManager().getEngineByName("JavaScript")

    // 状态管理器，存储字段的历史数据及其时间戳
    val numMap = mutable.Map[String, List[(Any, Long)]](
      "isStrictlyIncreasing(infoValue2)" -> List((20, 1000L)),
      "isStrictlyDecreasing(infoValue)" -> List((550, 1000L))
    )

    // 当前输入映射
    val inputMap02 = Map(
      "infoWTState" -> Some((false, 2000L)), // 值为 (Int, Long)
      "infoValue2" -> Some((30, 16000L)), // 值为 (Int, Long)
      "infoValue" -> Some((10, 16000L))    // 值为 (Int, Long)
    )

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

    // 动态表达式
    val replaceExp =
      """
        |((infoWTState == 10) || (infoValue == 10))
        |&&(isStrictlyIncreasing(infoValue2))
        |&&(isStrictlyDecreasing(infoValue))
        |""".stripMargin

    val num02 =extractFunctionCallsFromExpr(replaceExp)
    println(num02)
    // 替换表达式中的变量
    import scala.collection.mutable

    val numMap02: mutable.Map[String, List[(Any, Long)]] = mutable.Map.empty
//    val replacedExpr = replaceKeysInExpr(exr, inputMap, numMap02)
//    val replacedExpr02 = replaceKeysInExpr(replaceExp, inputMap02, numMap)
//    println(s"替换之后的数据：$replacedExpr")
//    println(s"打印替换02的表达式：$replacedExpr02")

    try {
      // 设置当前表达式上下文
      engine.getContext.setAttribute("currentExpr", replaceExp, javax.script.ScriptContext.ENGINE_SCOPE)

      // 执行表达式
      val replacedExpr="(0.0!= 9 && 0.0!=6) && (false == false) && \n (isStrictlyIncreasing([0.0])  && isStrictlyConstant([0.0]) )&&(isStrictlyConstant([0.0]))&&\n (false == false &&  false == false) && (false == false && false == false && false == false && false == false) && \n (false == false)"
      val result = engine.eval(replacedExpr)
      println(s"表达式结果: $result") // 输出：true 或 false
    } catch {
      case e: Exception => println(s"表达式执行失败: ${e.getMessage}")
    }

    // 打印最终的状态管理器内容
    println(s"最终状态管理器内容: $numMap")
  }
}