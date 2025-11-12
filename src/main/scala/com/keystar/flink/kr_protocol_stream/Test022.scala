package com.keystar.flink.kr_protocol_stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.util.Try

object Test022 {
  def main(args: Array[String]): Unit = {
    val num =toDouble("-0.01")
    println(num)
  }
  def toDouble(value: Any): Option[Double] = value match {
    case Some(v) => toDouble(v) // 递归调用 toDouble 处理 Option 内部的值
    case n: Number => Some(n.doubleValue())
    case s: String => Try(s.toDouble).toOption
    case _ =>
      None
  }
}
