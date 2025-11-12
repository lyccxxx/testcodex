package Test

import play.api.libs.json._ // 引入 Play JSON 库

import org.rocksdb._

object RocksTest {

  var rocksDB: RocksDB = _

  val path: String = "F:\\rocksdb"

  def testOpen(): Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCreateIfMissing(true)
      rocksDB = RocksDB.open(options, path)
      val testMap:Map[String,Map[String,List[Double]]]=Map("hello"->Map("world"->List(1.0,2.0)))
      val testMap01:Map[String,Map[String,List[Double]]]=Map("hello"->Map("world2"->List(3.2,4.1)))

      // 将 Map 序列化为 JSON 字符串
      implicit val listDoubleWrites: Writes[List[Double]] = Writes[List[Double]](list => JsArray(list.map(JsNumber(_))))
      implicit val mapStringMapListDoubleWrites: Writes[Map[String, Map[String, List[Double]]]] = Writes { map =>
        JsObject(map.map { case (k1, v1) =>
          k1 -> JsObject(v1.map { case (k2, v2) => k2 -> JsArray(v2.map(JsNumber(_))) })
        })
      }
      val jsonString: String = Json.toJson(testMap).toString()
      val jsonString01: String = Json.toJson(testMap01).toString()

      // 将 JSON 字符串存储到 RocksDB 中
      rocksDB.put("testMapKey".getBytes, jsonString.getBytes)
      rocksDB.put("testMapKey".getBytes, jsonString01.getBytes)
//      rocksDB.put("key".getBytes, "val".getBytes)

      val bytes: Array[Byte] = rocksDB.get("testMapKey".getBytes)
      println(new String(bytes))
    } catch {
      case e: RocksDBException => e.printStackTrace()
    } finally {
      // 确保在测试结束后关闭数据库连接（如果需要）
      if (rocksDB != null) {
        rocksDB.close()
      }
    }
  }

  def addArrays(arr1: JsArray, arr2: JsArray): JsArray = {
    val maxLength = Math.max(arr1.value.length, arr2.value.length)
    val result = (0 until maxLength).map { i =>
      val num1 = if (i < arr1.value.length) arr1.value(i).asOpt[JsNumber].map(_.value.toDouble).getOrElse(0.0) else 0.0
      val num2 = if (i < arr2.value.length) arr2.value(i).asOpt[JsNumber].map(_.value.toDouble).getOrElse(0.0) else 0.0
      JsNumber(num1 + num2)
    }
    JsArray(result)
  }

  def main(args: Array[String]): Unit = {
    testOpen()
  }
}