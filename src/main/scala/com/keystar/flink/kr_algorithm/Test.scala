import scala.collection.mutable.ListBuffer
import java.util

// 定义AlgorithResult样例类
case class AlgorithResult(
                           timestamp: Long,                // 时刻
                           expressions: String,            // 测点
                           collectedValue: Option[Any],    // 计算值
                           alarm: Boolean,                 // 持续时长告警
                           equip_label: String,
                           algorithm_id: Long,
                           station: String                 // 站点
                         )
// 测试对象
object Test {
  def main(args: Array[String]): Unit = {
    // 创建测试用的messageBuffer
    val messageBuffer = new util.ArrayList[AlgorithResult]()

    // 测试场景1: 先添加1值，再添加0值，预期两个情况都被删除
    println("=== 测试场景1: 先添加1值，再添加0值，预期两个情况都被删除 ===")
    val test1Record1 = AlgorithResult(1755625425000L, "A11_40_isCLXWenKongAbnorm", Some(1), alarm = true, "A11_40", 20043, "1521714652626283467")
    val test1Record2 = AlgorithResult(1755625425000L, "A11_40_isCLXWenKongAbnorm", Some(0), alarm = true, "A11_40", 20043, "1521714652626283467")

    invoke(test1Record1, messageBuffer)
    println(s"添加1值后: ${formatBuffer(messageBuffer)}")

    invoke(test1Record2, messageBuffer)
    println(s"添加0值后: ${formatBuffer(messageBuffer)}")
    println()

    // 清空缓冲区，准备下一个测试
    messageBuffer.clear()

    // 测试场景2: 先添加0值，再添加1值，预期0值保留
    println("=== 测试场景2:  先添加0值，再添加1值 ===")
    val test2Record1 = AlgorithResult(1755625425000L, "A8_31_isCLXWenKongAbnorm", Some(0), alarm = true, "A8_31", 20045, "1521714652626283467")
    val test2Record2 = AlgorithResult(1755625425000L, "A8_31_isCLXWenKongAbnorm", Some(1), alarm = true, "A8_31", 20045, "1521714652626283467")

    invoke(test2Record1, messageBuffer)
    println(s"添加0值后: ${formatBuffer(messageBuffer)}")

    invoke(test2Record2, messageBuffer)
    println(s"添加1值后: ${formatBuffer(messageBuffer)}")
    println()

    // 清空缓冲区，准备下一个测试
    messageBuffer.clear()

    // 测试场景3: 相同点位时间戳，添加2值后再添加3值，预期保留先添加的2值
    println("=== 测试场景3: 非0值之间不替换 ===")
    val test3Record1 = AlgorithResult(1755625425000L, "C2_06_isCLXWenKongAbnorm", Some(2), alarm = true, "C2_06", 20048, "1521714652626283467")
    val test3Record2 = AlgorithResult(1755625425000L, "C2_06_isCLXWenKongAbnorm", Some(3), alarm = true, "C2_06", 20048, "1521714652626283467")

    invoke(test3Record1, messageBuffer)
    println(s"添加2值后: ${formatBuffer(messageBuffer)}")

    invoke(test3Record2, messageBuffer)
    println(s"添加3值后: ${formatBuffer(messageBuffer)}")
    println()

    // 清空缓冲区，准备下一个测试
    messageBuffer.clear()

    // 测试场景4: 不同时间戳，相同点位，都保留
    println("=== 测试场景4: 不同时间戳的相同点位都保留 ===")
    val test4Record1 = AlgorithResult(1755625425000L, "B4_10_isCLXWenKongAbnorm", Some(2), alarm = true, "B4_10", 20050, "1521714652626283467")
    val test4Record2 = AlgorithResult(1755625425008L, "B4_10_isCLXWenKongAbnorm", Some(1), alarm = true, "B4_10", 20050, "1521714652626283467")

    invoke(test4Record1, messageBuffer)
    println(s"添加时间戳1755625425000L的2值后: ${formatBuffer(messageBuffer)}")

    invoke(test4Record2, messageBuffer)
    println(s"添加时间戳1755625425008L的1值后: ${formatBuffer(messageBuffer)}")
    println()

    // 清空缓冲区，准备下一个测试
    messageBuffer.clear()

    // 测试场景5: 相同时间戳，不同点位，都保留
    println("=== 测试场景5: 相同时间戳的不同点位都保留 ===")
    val test5Record1 = AlgorithResult(1755625425009L, "B1_17_isCLXWenKongAbnorm", Some(1), alarm = true, "B1_17", 20052, "1521714652626283467")
    val test5Record2 = AlgorithResult(1755625425009L, "C1_17_isCLXWenKongAbnorm", Some(1), alarm = true, "C1_17", 20057, "1521714652626283467")

    invoke(test5Record1, messageBuffer)
    println(s"添加point5的1值后: ${formatBuffer(messageBuffer)}")

    invoke(test5Record2, messageBuffer)
    println(s"添加point6的1值后: ${formatBuffer(messageBuffer)}")
  }

  // 格式化缓冲区输出，便于查看
  def formatBuffer(buffer: util.ArrayList[AlgorithResult]): String = {
    import scala.collection.JavaConverters._
    buffer.asScala.map { record =>
      s"[station: ${record.station}, expressions: ${record.expressions}, " +
        s"timestamp: ${record.timestamp}, value: ${record.collectedValue.getOrElse("None")}]"
    }.mkString(", ")
  }

  // 判断是否为数值类型
  def isNumericValue(value: AlgorithResult): Boolean = {
    value.collectedValue match {
      case Some(_: Number) => true
      case _ => false
    }
  }

  // 被测试的invoke方法
  def invoke(value: AlgorithResult, messageBuffer: util.ArrayList[AlgorithResult]): Unit = {
    if (value != null && value.alarm && isNumericValue(value)) {
      // 检查collectedValue是否为0、1、2、3中的一个
      value.collectedValue.foreach {
        case num: Number =>
          val numericValue = num.doubleValue()
          if (numericValue >= 0 && numericValue <= 3) {
            // 1. 计算当前数据的点位标识（station + expressions）和时间戳
            val currentKey = if (value.expressions.isEmpty) s"${value.station}" else s"${value.station}.${value.expressions}"
            val currentTs = value.timestamp

            // 2. 遍历缓冲区，查找相同点位且相同时间戳（时间段）的已有数据
            import scala.collection.JavaConverters._
            var existingRecord: Option[AlgorithResult] = None
            val iterator = messageBuffer.iterator()
            while (iterator.hasNext && existingRecord.isEmpty) {
              val record = iterator.next()
              val recordKey = if (record.expressions.isEmpty) s"${record.station}" else s"${record.station}.${record.expressions}"
              val recordTs = record.timestamp
              // 点位相同且时间戳（时间段）相同
              if (recordKey == currentKey && recordTs == currentTs) {
                existingRecord = Some(record)
              }
            }

            //            // 3. 根据已有记录和当前记录的collectValue进行优先级判断（优先保留0值）
            //            existingRecord match {
            //              case Some(existing) =>
            //                // 3.1 提取已有记录的数值
            //                val existingValue = existing.collectedValue match {
            //                  case Some(n: Number) => n.doubleValue()
            //                  case _ => -1.0 // 无效值，按非0处理
            //                }
            //
            //                if (numericValue == 0.0) {
            //                  // 3.2 当前值为0，优先级最高，移除已有记录并添加当前记录
            //                  messageBuffer.remove(existing)
            //                  messageBuffer.add(value)
            //                } else if (existingValue != 0.0) {
            //                  // 3.3 两者都不是0，保留已有记录（避免重复非0值）
            //                }
            //              // 3.4 若已有记录是0，则不做操作（0优先级最高）
            //
            //              case None =>
            //                // 4. 无相同点位和时间段的记录，直接添加当前记录
            //                messageBuffer.add(value)
            //            }
            //          }
            //        case _ => // 非数值类型忽略
            //      }
            //    }
            //  }
            //}
            // 3. 根据已有记录和当前记录的collectValue进行优先级判断
            existingRecord match {
              case Some(existing) =>
                // 3.1 提取已有记录的数值
                val existingValue = existing.collectedValue match {
                  case Some(n: Number) => n.doubleValue()
                  case _ => -1.0 // 无效值，按非0处理
                }

                // 修改的逻辑：如果先有1后有0，则删除两条记录
                if (numericValue == 0.0 && existingValue == 1.0) {
                  // 当前值为0且已有记录为1，删除两条记录
                  messageBuffer.remove(existing)
                  // 不添加当前记录，实现两条都删除
                } else if (numericValue == 0.0) {
                  // 当前值为0（但不是针对1的），优先级最高，移除已有记录并添加当前记录
                  messageBuffer.remove(existing)
                  messageBuffer.add(value)
                } else if (existingValue != 0.0) {
                  // 两者都不是0，保留已有记录（避免重复非0值）
                  // 不做任何操作
                }
              // 若已有记录是0，则不做操作（0优先级最高）

              case None =>
                // 4. 无相同点位和时间段的记录，直接添加当前记录
                messageBuffer.add(value)
            }
          }
        case _ => // 非数值类型忽略
      }
    }
  }
}
