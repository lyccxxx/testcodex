package com.keystar.flink.flink_stream

import javax.script.ScriptEngineManager

object Test01 {
  def main(args: Array[String]): Unit = {
    // 示例数据
    val data: Map[String, List[(Option[Double], Some[Long])]] = Map(
      "key1" -> List((Some(1.0), Some(100L)), (Some(2.0), Some(200L))),
      "key2" -> List((Some(3.0), Some(300L)), (Some(4.0), Some(400L)))
    )

    // 目标 key 和 Some[Long]
    val targetKey = "key1"
    val targetLong = Some(200L)

    // 定位函数
    def findOptionDouble(
                          data: Map[String, List[(Option[Double], Some[Long])]],
                          targetKey: String,
                          targetLong: Some[Long]
                        ): Option[Double] = {
      // 1. 通过 key 获取对应的列表
      data.get(targetKey) match {
        case Some(list) =>
          // 2. 遍历列表，找到匹配的 Some[Long]
          list.collectFirst {
            case (Some(doubleValue), `targetLong`) => doubleValue
          }
        case None =>
          // 如果 key 不存在，返回 None
          None
      }
    }

    // 调用函数
    val result = findOptionDouble(data, targetKey, targetLong)
    //获取到管理器里边的数据
    //在这一块进行多测点的判断：
    //先获取到最新的死值，死值存储的是时间戳和具体值
    //从data业务数据里边拿到时间戳
//    for ((key, value) <- data.values) {
      // 使用 . 分割字符串
//      val parts = key.split("\\.")
      // 获取最后一部分即测点
//      val result = parts.lastOption.getOrElse("") //测点字段名称
//      val rule = rulesForDevice.find(_.iot_fld == result) //规则
      //从规则里取json
//      rule match {
//        case Some(diagnosisRule) =>
//          //取出规则json
//          val t_json = diagnosisRule.info_wt_state_json
//          //获取到具体的值
//          parseJson(t_json,data.timestamp,result)
//        case None =>
//          // 处理没有值的情况
//          println("No diagnosis rule found.")
//      }
//    }
//    // 打印结果
//    println(result) // 输出: Some(2.0)
  }
}
