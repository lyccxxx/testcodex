package Test

object Test04 {
  def main(args: Array[String]): Unit = {
    val str_num =Some(null)
    if(str_num.exists(_ != null)){
      println("有值")
    }else{
      println("没有值")
    }
  }
}
