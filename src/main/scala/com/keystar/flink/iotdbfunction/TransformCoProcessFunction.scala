package com.keystar.flink.iotdbfunction


import com.keystar.flink.iotdbfunction.IotdbFunction.{Diagnostic_method, diagnoseDeadValueFault}
import com.keystar.flink.iotdbstream.{DiagnosisRule, IoTDBReading}
import com.keystar.flink.iotdbstream.IoTDBSource.{handleResponse, iotdb_url, sendRequest}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import play.api.libs.json.{JsArray, JsObject, Json}

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}
import scala.collection.JavaConverters._
import java.sql.Timestamp
import java.time.Instant
import javax.script.ScriptEngineManager
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, DurationInt}

class TransformCoProcessFunction(outputTag: OutputTag[DiagnosisResult],id:Int) extends  CoProcessFunction[(String, String),DiagnosisRule, DiagnosisResult] {
  @transient private var isPgSourceLoaded: Boolean = false
  //定义计时字段
  @transient private var update_dt: Timestamp = _
  @transient private var lastNum = 0
  @transient private var num = 0
  @transient private var executor: ExecutorService = _
  @transient private var latch: CountDownLatch = _
  @transient private var lastRegisteredTimer: Long = -1L
  //定义一个状态管理器存储规则数据，按照站点设备进行存储
  private lazy val rules: ValueState[Map[String, Set[DiagnosisRule]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, Set[DiagnosisRule]]]("DiagnosisRuleValues", classOf[Map[String, Set[DiagnosisRule]]])
  )


  //定义一个记录规则数据是否加载完成的标识
  private var rulesState: ListState[Map[String, List[DiagnosisRule]]] = _


  // 状态管理器，用于存储未处理的 IoTDBReading 事件
  private lazy val pendingReadingsState: ListState[(String, String)] = getRuntimeContext.getListState(
    new ListStateDescriptor[(String, String)]("pendingReadings", classOf[(String, String)])
  )


  // 状态管理器，用于存储每个测点的上一个有效值及其时间戳
  private lazy val lastValidValuesState: ValueState[Map[String, (Option[Any], Option[Long], Option[Double])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Any], Option[Long], Option[Double])]]("lastValidValues", classOf[Map[String, (Option[Any], Option[Long], Option[Double])]])
  )

  // 状态管理器，用于记录采样频率不符问题开始的时间戳
  private lazy val samplingFrequencyMismatchStart: ValueState[Map[String, (Option[Long], Option[Long])]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Option[Long], Option[Long])]]("samplingFrequencyMismatchStart", classOf[Map[String, (Option[Long], Option[Long])]])
  )

  // 状态管理器，用于记录站点、设备，上次跑的时间
  private lazy val facilityStartTime: ValueState[Map[String, Option[Long]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, Option[Long]]]("facilityStartTime", classOf[Map[String, Option[Long]]])
  )

  // 状态管理器，用于记录每个传感器的死值故障状态
  private lazy val dedfaultState: ValueState[Map[String, List[(Option[Double], Some[Long])]]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, List[(Option[Double], Some[Long])]]]("dedfaultState", classOf[Map[String, List[(Option[Double], Some[Long])]]])
  )


  // 状态管理器，用于记录故障状态
  private lazy val faultStates: ValueState[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]]("faultStates", classOf[Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)]])
  )



  // 定义状态描述符，用于存储 DiagnosticsResult 列表
  private lazy val resultStateDescriptor: ListState[DiagnosisResult] = getRuntimeContext.getListState(
    new ListStateDescriptor[DiagnosisResult]("resultState", classOf[DiagnosisResult]))

  override def open(parameters: Configuration): Unit = {
    rulesState = getRuntimeContext.getListState(
      new ListStateDescriptor[Map[String, List[DiagnosisRule]]]("DiagnosisRule", classOf[Map[String, List[DiagnosisRule]]])
    )
    // 初始化固定大小的线程池
    executor = Executors.newFixedThreadPool(5) // 根据需要调整线程数量
    update_dt = new Timestamp(0)
  }

  override def processElement1(iotData: (String, String), ctx: CoProcessFunction[(String, String), DiagnosisRule, DiagnosisResult]#Context, out: Collector[DiagnosisResult]): Unit = {
    val start = System.currentTimeMillis()
    val (device, field) = iotData
    val ruleKey = device + "_" + field
    // 获取当前的状态
    val lastValidValues = Option(lastValidValuesState.value()).getOrElse(Map.empty[String, (Option[Any], Option[Long], Option[Double])])

    val mismatchStart = Option(samplingFrequencyMismatchStart.value()).getOrElse(Map.empty[String, (Option[Long], Option[Long])])

    var faultStatesMap = Option(faultStates.value()).getOrElse(Map.empty[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)])
    //死值故障标识
    val dedfaultstate = Option(dedfaultState.value()).getOrElse(Map.empty[String, List[(Option[Double], Some[Long])]])

    //设备时间
    val faStartime = Option(facilityStartTime.value()).getOrElse(Map.empty[String, Some[Long]])

    // 获取规则数据
    val currentRules = rulesState.get().asScala.foldLeft(Map.empty[String, List[DiagnosisRule]]) { (acc, map) =>
      acc ++ map
    }
    // 获取所有键的状态
    val rulesForDevice = currentRules.getOrElse(ruleKey, Set.empty[DiagnosisRule])

    if (rulesForDevice.nonEmpty && isPgSourceLoaded) {
      val iotFlds = rulesForDevice.map(_.iot_fld)
      val startTimes = System.currentTimeMillis()
      if (iotFlds.size > 50000) {
        val startTime = System.currentTimeMillis()
        //获取线程数量
        val poolSize = executor.asInstanceOf[java.util.concurrent.ThreadPoolExecutor].getCorePoolSize
        // 定义每个批次的大小
        val batchSize2 = if (iotFlds.size > 5000) 1000 else math.max(1, (iotFlds.size.toDouble / poolSize).ceil.toInt)
        val batchSize = 1000
        val numBatches = math.ceil(iotFlds.size.toDouble / batchSize).toInt
        latch = new CountDownLatch(numBatches)
        val resultsCollector = new java.util.concurrent.ConcurrentLinkedQueue[DiagnosisResult]()
        val futures: Seq[Future[Unit]] = (0 until numBatches).map { i =>
          val start = i * batchSize
          val end = (i + 1) * batchSize
          val batch = iotFlds.slice(start, end.min(iotFlds.size)).mkString(",")
          val sql = getSqlQuery(faStartime.get(ruleKey), batch, device)
          Future {
            var numProcessed = 0
            try {
              val result = sendRequest(sql)
              val respond = handleResponse(result)

              if (respond != null && respond.nonEmpty) {
                respond.foreach(data => {
                  numProcessed += 1
                  // 创建诊断结果对象
                  val diagnosisResult = handleIotDBdata(data, rulesForDevice, lastValidValues, mismatchStart, faultStatesMap, dedfaultstate)
                  // 添加到线程安全的结果收集器
                  resultsCollector.add(diagnosisResult)
                })
              }
            } catch {
              case e: Exception =>
                println(s"输出异常 ${Thread.currentThread().getId} $e")
            } finally {
              // 记录处理时间和数量
              val endTime = System.currentTimeMillis()
              val duration = (endTime - startTime) / 1000.0
              println(s"线程 ${Thread.currentThread().getId} 处理了 ${batch.size} 个字段，耗时 ${duration} 秒, 处理的num: $numProcessed")
              // 每个任务完成后计数器减一
              latch.countDown()
            }
          }
        }
        // 等待所有任务完成或达到超时时间
        try {
          val timeoutDuration = 5.minutes
          Await.result(Future.sequence(futures), timeoutDuration)
          println("所有任务已完成")
        } catch {
          case te: TimeoutException =>
            println("有任务未完成，在规定时间内未能完成所有任务")
          // 处理超时逻辑
        } finally {
          executor.shutdown()
        }
        // 等待所有批次处理完毕
        // 等待所有任务完成或达到超时时间
        val result = resultsCollector.asScala.toList
        result.foreach(out.collect)
      } else {
        val iotFlds01 = rulesForDevice.map(_.iot_fld)
        if (iotFlds01.nonEmpty) {
          val batchSize = 1000
          val numBatches = math.ceil(rulesForDevice.size.toDouble / batchSize).toInt
          for (i <- 0 until numBatches) {
            val startIdx = i * batchSize
            val endIdx = (i + 1) * batchSize.min(rulesForDevice.size)
            val batchRules = rulesForDevice.slice(startIdx, endIdx)
            val iotFldsBatch = batchRules.map(_.iot_fld).mkString(",")
            val device = batchRules.head.iot_tbl // 假设所有规则有相同的设备名
            val sql = getSqlQuery(faStartime.get(ruleKey), iotFldsBatch, device)
//            println(s"打印sql:$sql")
            val result = sendRequest(sql)
            val responses = handleResponse(result)
            if (responses.nonEmpty) {
              val timestamp = responses.map(data => data.timestamp).max
              facilityStartTime.update(Map(ruleKey -> Some(timestamp)))
            }
            println(s"打印线程：${Thread.currentThread().getId}")
            responses.foreach(data => {
              val mismatchStart2 = Option(samplingFrequencyMismatchStart.value()).getOrElse(Map.empty[String, (Option[Long], Option[Long])])
              val faultStatesMap2 = Option(faultStates.value()).getOrElse(Map.empty[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)])
              //              println(s"打印map集合情况:$faultStatesMap2")
              val dedfaultstate2 = Option(dedfaultState.value()).getOrElse(Map.empty[String, List[(Option[Double], Some[Long])]])
              val lastValidValues2 = Option(lastValidValuesState.value()).getOrElse(Map.empty[String, (Option[Any], Option[Long], Option[Double])])
              val diagnosisResult = handleIotDBdata(data, rulesForDevice, lastValidValues2, mismatchStart2, faultStatesMap2, dedfaultstate2)
              if(diagnosisResult.faultDescription!="0b0000"){
                ctx.output(outputTag, diagnosisResult)
              }else{
                out.collect(diagnosisResult)
              }
            })
          }
        }
      }
    } else {
      pendingReadingsState.add(iotData)
    }
    val end = System.currentTimeMillis() - start
    //    println(s"打印处理的设备 $ruleKey 以及时间 $end  线程：${Thread.currentThread().getId}")
  }

  override def processElement2(rule: DiagnosisRule, ctx: CoProcessFunction[(String, String), DiagnosisRule, DiagnosisResult]#Context, out: Collector[DiagnosisResult]): Unit = {
    val sub_num = getNum(id)
    val ruleKey = if (id == 3) rule.iot_tbl + "_" + rule.iot_dev else rule.iot_tbl + "_" + rule.iot_fld.substring(0, sub_num)
    val currentRules = rulesState.get().asScala.foldLeft(Map.empty[String, List[DiagnosisRule]]) { (acc, map) =>
      acc ++ map
    }
    val updatedRules = updateRules(ruleKey, rule)
    rulesState.clear()
    rulesState.add(updatedRules)
    num = currentRules.size
    // 设置一个定时器，在5秒后检查数据流状态
    if (lastNum < num) {
      lastNum = num
    } else {
      registerTimer(ctx)
    }
  }

  // 定义 onTimer 方法来处理定时器的触发
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, String), DiagnosisRule, DiagnosisResult]#OnTimerContext, out: Collector[DiagnosisResult]): Unit = {
    if (lastNum == num) {
      isPgSourceLoaded = true
    }
  }

  override def close(): Unit = {
    executor.shutdownNow() // 确保线程池也被正确关闭
  }
  // 定位函数
  def findOptionDouble(
                        data: Map[String, List[(Option[Double], Some[Long])]],
                        targetKey: String,
                        targetLong: Long // 改为直接使用 Long 类型而非 Some[Long]
                      ): Option[Double] = {
    // 1. 通过 key 获取对应的列表
    data.get(targetKey) match {
      case Some(list) =>
        // 2. 遍历列表，找到匹配的 Long 值，并返回对应的 Option[Double]
        list.collectFirst {
          case (doubleValue, Some(longValue)) if longValue == targetLong => doubleValue
        }.getOrElse(None) // 如果没有找到匹配项，则返回 None
      case None =>
        // 如果 key 不存在，返回 None
        None
    }
  }
  private def parseJson(a:String,time:Long,iot_fld:String): Unit = {
    val dedfaultstate3 = Option(dedfaultState.value()).getOrElse(Map.empty[String, List[(Option[Double], Some[Long])]])
    //从死值中取其物理值
    val json = Json.parse(a)
    json match {
      case obj: JsObject if obj.fields.isEmpty => println("The JSON object is empty.")
      case arr: JsArray if arr.value.isEmpty => println("The JSON array is empty.")
      case _ =>
        // 获取 rule 对象
        (json \ "rule").asOpt[JsObject] match {
          case Some(rule) =>
            val inputData = (rule \ "input_data").asOpt[List[JsObject]].getOrElse(List())
            //获取json里边所有的点位
            val arr_position:List[String]=inputData.map{data=>
              val iot_fld = (data\"position").as[String]
              iot_fld
            }
            // 检查所有的点位是否都能取到正常的 Double 值
            val arr_bool: List[Option[Double]] = arr_position.map { position =>
              findOptionDouble(dedfaultstate3, position, time)
            }
            // 判断是否有任何点位未能获取到有效的 Double 值
            if (arr_bool.exists(_.isEmpty)) {
              println("Not all positions have valid Double values. Stopping further calculations.")
            } else {
              // 执行下一步的计算
              val validValues: List[Double] = arr_bool.map(_.get) // 将 Option[Double] 转换为 Double
              println(s"All positions have valid Double values: $validValues")
              //这里还需要用到那个值
              val inputMap = inputData.map { data =>
                val attr = "/"+(data \ "attr").as[String]
                val position = (data\"position").as[String]
                val valueString = (data \ "value").as[String]
                val value = if (valueString == null || valueString.isEmpty) {
                  findOptionDouble(dedfaultstate3, position, time) // 或者其他默认值
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
              //开始往下游进行输出
              // 组合输出的下游数据
            }
          case None => println("No 'rule' field found in the JSON.")
        }
    }
  }

  private def getNum(id: Int): Int = {
    val column = id match {
      case 1 => 8
      case 2 => 5
      case 3 => 4
      case 4 => 8
      case _ => 0
    }
    column
  }

  private def registerTimer(ctx: CoProcessFunction[(String, String), DiagnosisRule, DiagnosisResult]#Context): Unit = {
    val timerTime = ctx.timerService().currentProcessingTime() + 500L
    ctx.timerService().registerProcessingTimeTimer(timerTime)
    lastRegisteredTimer = timerTime
  }

  private def updateRules(ruleKey: String, rule: DiagnosisRule): Map[String, List[DiagnosisRule]] = {
    val currentRules = rulesState.get().asScala.foldLeft(Map.empty[String, List[DiagnosisRule]]) { (acc, map) =>
      acc ++ map
    }
    currentRules.get(ruleKey) match {
      case Some(existingRules) => currentRules + (ruleKey -> (rule :: existingRules))
      case None => currentRules + (ruleKey -> List(rule))
    }
  }


  def getSqlQuery(lastTsOpt: Option[Option[Long]], iotFlds: String, device: String): String = {
    lastTsOpt match {
      case Some(lastTs) =>
        val nextStartTime = Instant.ofEpochMilli(lastTs.getOrElse(0L)).plusSeconds(15).toEpochMilli
        val nextEndTime = Instant.ofEpochMilli(lastTs.getOrElse(0L)).plusSeconds(90).toEpochMilli
        s"SELECT ${iotFlds} FROM ${device} WHERE time >= ${nextStartTime} AND time < ${nextEndTime}"
      case None =>
        s"SELECT ${iotFlds} FROM ${device} where time < 1721974305000 and  time>=1721887905000"
    }
  }


  def handleIotDBdata(data: IoTDBReading,
                      rulesForDevice: Iterable[DiagnosisRule],
                      lastValidValues: Map[String, (Option[Any], Option[Long], Option[Double])],
                      mismatchStart: Map[String, (Option[Long], Option[Long])],
                      faultStatesMap: Map[String, (Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Option[Long], Option[Long], Boolean, Boolean)],
                      dedfaultstate: Map[String, List[(Option[Double], Some[Long])]]
                     ): DiagnosisResult = {
    var diagnosisResult = DiagnosisResult(timestamp = 0, expressions = ""
      , collectedValue = None, faultDescription = ""
      , validValue = None, lastValidTimestamp = None)
    for ((key, value) <- data.values) {
      // 使用 . 分割字符串
      val parts = key.split("\\.")
      // 获取最后一部分即测点
      val result = parts.lastOption.getOrElse("")
      val rule = rulesForDevice.find(_.iot_fld == result)
      rule match {
        case Some(r) =>
          val (lastValue, samFrStart, faultMap, dedValues, sampCheck) = Diagnostic_method(key = key, value = value, iotData = data, rule = r
            , lastValidValues = lastValidValues
            , samplingFrequencyMismatchStart = mismatchStart
            , faultStatesMap = faultStatesMap
            , dedValidValues = dedfaultstate)
          lastValidValuesState.update(lastValue)
          samplingFrequencyMismatchStart.update(samFrStart)
          faultStates.update(faultMap)
          dedfaultState.update(dedValues)
          val (maxbool, _, _, minbool, _, _, ratebool, _, _, dedbool, lastbool) = faultMap.getOrElse(key, (false, None, None, false, None, None, false, None, None, false, false))
          var faultDescription = "0b0000"
          if (sampCheck) {
            faultDescription = "0b010000"
          } else if (maxbool) {
            faultDescription = "0b1000"
          } else if (minbool) {
            faultDescription = "0b0100"
          } else if (ratebool) {
            faultDescription = "0b1001"
          } else if (dedbool) {
            faultDescription = "0b0001"
          } else if (lastbool) {
            faultDescription = "0b1111"
          } else {
            faultDescription = "0b0000"
          }
          val validValue = lastValue.getOrElse(key, (None, None, None))._1
          val lastTimestamp2 = lastValidValues.getOrElse(key, (None, None, None))._2
          diagnosisResult = DiagnosisResult(
            timestamp = data.timestamp,
            expressions = key,
            collectedValue = value,
            faultDescription = faultDescription,
            validValue = validValue,
            lastValidTimestamp = lastTimestamp2
          )
        case None =>
          val lastTimestamp2: Option[Long] = lastValidValues.getOrElse(key, (None, None, None))._2
          if (lastTimestamp2.isDefined) {
            val updatedValues = lastValidValues + (key -> (lastValidValues.getOrElse(key, (None, None, None))._1, Some(data.timestamp), None))
            lastValidValuesState.update(updatedValues)
            //测点存在上一条数据
            diagnosisResult = DiagnosisResult(
              timestamp = data.timestamp,
              expressions = key,
              collectedValue = value,
              faultDescription = "没有该节点的规则",
              validValue = lastValidValues.getOrElse(key, (None, None, None))._1,
              lastValidTimestamp = lastTimestamp2
            )
          } else {
            // 如果没有上一个时间戳，说明这是第一个数据点，直接记录当前时间戳
            val updatedValues = lastValidValues + (key -> (None, Some(data.timestamp), None))
            lastValidValuesState.update(updatedValues)
            diagnosisResult = DiagnosisResult(
              timestamp = data.timestamp,
              expressions = key,
              collectedValue = value,
              faultDescription = "没有该节点的规则",
              validValue = None,
              lastValidTimestamp = None
            )
          }
      }
    }
    diagnosisResult
  }
}