package com.keystar.flink.kr_algorithm

import org.apache.flink.util.Collector

import scala.collection.mutable

object ProcessAndOutput {
  def main(args: Array[String]): Unit = {
    // outputMap配置：position="pos1" 无需持续检查（durationStatus=false），持续时间30秒
    val outputMap1: Map[String, (Int, Boolean, Long, String)] = Map(
      "pos1" -> (30,true, 1001L, "equip_1")  // (durationSeconds=30, durationStatus=false, algorithm_id=1001, equip_label="equip_1")
    )

    // proData数据：包含3条不同值的记录
    val proData1: Seq[(String, String, Option[Any], Long, String)] = Seq(
      // 计算值为true（布尔值）
      ("expr1", "replaced_expr1", Some(1), 1620000000000L, "pos1"),  // 时间戳：2021-05-03 00:00:00
      // 计算值为1（视为true）
      ("expr1", "replaced_expr1", Some(1), 1620000015000L, "pos1"),      // 时间戳+15秒
      // 计算值为0（视为false）
      ("expr1", "replaced_expr1", Some(1), 1620000030000L, "pos1")       // 时间戳+30秒
    )

    val key1: String = "site_1"  // 站点标识
    processAndOutput111(proData1, outputMap1, key1)


  }
  def processAndOutput111(
                        proData: Seq[(String, String, Option[Any], Long, String)],
                        outputMap: Map[String, (Int, Boolean, Long, String)],
                        key: String
//                        out: Collector[AlgorithResult]
                      ): Unit = {
    val continue_data: mutable.Map[String, List[(Any, Long)]] = mutable.Map.empty

    proData.foreach { case (_, _, maybeValue, timestamp, position) =>
      outputMap.get(position).foreach { case (durationSeconds, durationStatus, algorithm_id, equip_label) =>
        maybeValue.foreach { collectedValue =>
          // 转换为布尔值（判断当前是否满足告警条件）
          val valueAsBoolean = collectedValue match {
            case bool: Boolean => bool
            case num: Number => num.intValue() == 1 || num.intValue() == 2
            case _ => false
          }

          if (!durationStatus) {
            // 无需持续检查：直接输出（逻辑不变）
            val alarm = valueAsBoolean
//            out.collect(AlgorithResult(
//              timestamp = timestamp,
//              expressions = position,
//              collectedValue = Some(collectedValue),
//              alarm =false,
//              station = key,
//              equip_label = equip_label,
//              algorithm_id = algorithm_id
//            ))
          } else {
            // 需要持续检查：调整状态管理逻辑
            val expKey = s"$key-$position"
            val requiredPoints = durationSeconds / 15  // 所需点数（每15秒一个点）

            // 1. 安全获取当前状态（不存在则为空列表）
            val currentState = continue_data.getOrElse(expKey, Nil)
//            val currentState = Option(continue_data.get(expKey)).getOrElse(Nil)

            if (!valueAsBoolean) {
              // 2. 当前值为false：检查是否需要输出最后一次告警，然后清空状态
              if (currentState.nonEmpty) {
                // 若历史数据长度达到所需点数，输出一次告警（视为结束前的最后一次）
                if (currentState.length == requiredPoints) {
//                  out.collect(AlgorithResult(
//                    timestamp = timestamp,
//                    expressions = position,
//                    collectedValue = Some(collectedValue),
//                    alarm = true,
//                    station = key,
//                    equip_label = equip_label,
//                    algorithm_id = algorithm_id
//                  ))
                }
                continue_data.remove(expKey)  // 清空状态
              }
              // 输出当前非告警
//              out.collect(AlgorithResult(
//                timestamp = timestamp,
//                expressions = position,
//                collectedValue = Some(collectedValue),
//                alarm = false,
//                station = key,
//                equip_label = equip_label,
//                algorithm_id = algorithm_id
//              ))
            } else {
              // 3. 当前值为true：处理状态更新与长度适配
              // 3.1 清理超时数据（仅保留cutoffTime之后的）
              val cutoffTime = timestamp - durationSeconds * 1000L  // 时间窗口下限（毫秒）
              val cleanedState = currentState.filter { case (_, ts) => ts >= cutoffTime }

              // 3.2 限制列表长度不超过requiredPoints（只保留最新的N个点）
              val truncatedState = if (cleanedState.length > requiredPoints) {
                cleanedState.takeRight(requiredPoints)  // 超过则截取最新的requiredPoints个
              } else {
                cleanedState
              }

              // 3.3 判断数据连续性（与上一个点间隔是否为15秒）
              val isContinuous = truncatedState.lastOption match {
                case Some((_, lastTs)) => (timestamp - lastTs) == 15 * 1000L
                case None => true  // 第一个点视为连续
              }

              if (isContinuous) {
                // 3.4 连续数据：更新状态（添加当前点，再截断到最大长度）
                val tempState = truncatedState :+ (collectedValue -> timestamp)
                val updatedState = if (tempState.length > requiredPoints) tempState.takeRight(requiredPoints) else tempState
                continue_data.put(expKey, updatedState)

                // 3.5 判断是否满足告警条件（长度达标且所有值有效）
                val allValuesValid = updatedState.forall { case (v, _) =>
                  v match {
                    case b: Boolean => b
                    case n: Number => n.intValue() == 1 || n.intValue() == 2
                    case _ => false
                  }
                }
                val alarm = updatedState.length >= requiredPoints && allValuesValid
                if(alarm) {println("满足告警条件")}
//                out.collect(AlgorithResult(
//                  timestamp = timestamp,
//                  expressions = position,
//                  collectedValue = Some(collectedValue),
//                  alarm = alarm,
//                  station = key,
//                  equip_label = equip_label,
//                  algorithm_id = algorithm_id
//                ))
              } else {
                // 3.6 不连续：重置状态为当前点（长度=1）
                // 先输出
//                out.collect(AlgorithResult(
//                  timestamp = timestamp,
//                  expressions = position,
//                  collectedValue = Some(collectedValue),
//                  alarm = false,
//                  station = key,
//                  equip_label = equip_label,
//                  algorithm_id = algorithm_id
//                ))
                // 清空历史状态，再添加状态
                val resetState = List((collectedValue -> timestamp))
                continue_data.remove(expKey)    // 先清空历史状态
                continue_data.put(expKey, resetState)    // 再添加状态

              }
            }
          }
        }
      }
    }
  }
  def processAndOutput(
                        proData: Seq[(String, String, Option[Any], Long, String)], // (表达式, 替换后表达式, 计算值, 时间戳, position)
                        outputMap: Map[String, (Int, Boolean,Long,String)], // position -> (持续时间秒数, 是否需要持续检查)
                        key: String  // 站点
                        //                      out: Collector[AlgorithResult] // 下游输出器
                      ): Unit = {

    val continueData: mutable.Map[String, List[(Any, Long)]] = mutable.Map.empty

    // 从proData的每个元素中提取出 5 个字段，其中前两个字段用_表示（不需要使用），后三个分别命名为maybeValue（数据值）、timestamp（时间戳）和position（位置标识）
    proData.foreach { case (_, _, maybeValue, timestamp, position) =>
      // 获取当前position的告警配置：(durationSeconds, durationStatus)
      outputMap.get(position).foreach { case (durationSeconds, durationStatus,algorithm_id,equip_label) =>
        maybeValue.foreach { collectedValue =>
          // 转换为布尔值（判断当前值是否满足告警条件）
          val valueAsBoolean = collectedValue match {
            case bool: Boolean => bool
            case num: Number => num.intValue() == 1 || num.intValue() == 2 // 1/2视为true
            case _ => false
          }
          // 先根据durationStatus判断是否需要持续时间检查
          if (!durationStatus) {
            // 无需持续检查：直接根据当前值判断告警
            val alarm = valueAsBoolean
            //          out.collect(AlgorithResult(
            //            timestamp = timestamp,
            //            expressions = position,
            //            collectedValue = Some(collectedValue),
            //            alarm = false,
            //            station = key,
            //            equip_label = equip_label,
            //            algorithm_id = algorithm_id
            //          ))
          } else {
            // 需要持续检查：执行原逻辑中的持续时间、连续性判断
            val expKey = s"$key-$position"

            // 当前值为false时，清空状态
            if (!valueAsBoolean && continueData.contains(expKey)) {
              continueData.remove(expKey)
            }

            if (valueAsBoolean) {
              // 获取当前状态（累积的数据列表）
              val currentState = continueData.getOrElse(expKey, Nil)

              // 清除超过durationSeconds的旧数据
              val cutoffTime = timestamp - durationSeconds * 1000L
              val cleanedState = currentState.filter { case (_, ts) => ts >= cutoffTime }

              // 判断是否与上一条数据连续（间隔15s）
              val isContinuous = cleanedState.lastOption match {
                case Some((_, lastTs)) => (timestamp - lastTs) == 15 * 1000L
                case None => true // 第一条数据视为连续
              }

              if (isContinuous) {
                // 数据连续，更新状态
                val updatedState = cleanedState :+ (collectedValue -> timestamp)
                continueData.put(expKey, updatedState)

                // 判断是否满足持续时间要求（数据点数量 >= 总时长/15s间隔）
                // 数据点数量=持续时间/15
                val requiredPoints = durationSeconds / 15
                // 判断所有数据点是否都有效
                val allValuesValid = updatedState.forall { case (v, _) =>
                  v match {
                    case b: Boolean => b
                    case n: Number => n.intValue() == 1 || n.intValue() == 2
                    case _ => false
                  }
                }

                // 满足条件则告警:持续时间数据点数大于要求点数（即持续时长满足要求）并且所有数据点有效
                val alarm = updatedState.size >= requiredPoints && allValuesValid
                if(alarm){
                  println(s"告警11：${timestamp}")
                }

                //              out.collect(AlgorithResult(
                //                timestamp = timestamp,
                //                expressions = position,
                //                collectedValue = Some(collectedValue),
                //                alarm = alarm,
                //                station = key,
                //                equip_label = equip_label,
                //                algorithm_id = algorithm_id
                //              ))
              } else {
                // 数据不连续
                // 先输出
                //              out.collect(AlgorithResult(
                //                timestamp = timestamp,
                //                expressions = position,
                //                collectedValue = Some(collectedValue),
                //                alarm = false, // 不连续时不告警
                //                station = key,
                //                equip_label = equip_label,
                //                algorithm_id = algorithm_id
                //              ))
                // 接着清空历史状态，添加新状态
                continueData.remove(expKey)   // 清空历史状态
                continueData.put(expKey, List((collectedValue -> timestamp)))  //添加新状态
              }
            } else {
              //            // 当前值为false，输出不告警
              //            out.collect(AlgorithResult(
              //              timestamp = timestamp,
              //              expressions = position,
              //              collectedValue = Some(collectedValue),
              //              alarm = false,
              //              station = key,
              //              equip_label = equip_label,
              //              algorithm_id = algorithm_id
              //            ))
            }
          }
        }
      }
    }
  }
}
