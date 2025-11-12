package com.keystar.flink.kr_protocol_stream

import org.slf4j.LoggerFactory

object MemoryMonitor {
  private val logger = LoggerFactory.getLogger(getClass)

  def logMemoryUsage(prefix: String = ""): Unit = {
    val runtime = Runtime.getRuntime
    val mb = 1024 * 1024

    val totalMemory = runtime.totalMemory / mb
    val freeMemory = runtime.freeMemory / mb
    val usedMemory = (totalMemory - freeMemory)

    logger.info(s"$prefix - Total: ${totalMemory}MB, Free: ${freeMemory}MB, Used: ${usedMemory}MB")
  }
}