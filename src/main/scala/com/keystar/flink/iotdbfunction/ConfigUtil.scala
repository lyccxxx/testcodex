package com.keystar.flink.iotdbfunction

import java.util.Properties

/**
 * @ClassName ConfigUtil
 * @Description TODO 加载配置文件工具类
 * @Author MrLu
 * @Date 2024年11月20日 09:34
 * @Version
 **/
object ConfigUtil {

  def getProperties(key: String): AnyRef = {
    val prop = new Properties()
    // 绑定配置文件
    val inputStream = ConfigUtil.getClass.getClassLoader.getResourceAsStream("config.properties")
    prop.load(inputStream)
    val value = prop.get(key)
    value
  }
}
