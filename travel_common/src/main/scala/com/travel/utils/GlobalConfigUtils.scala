package com.travel.utils

import java.util.ResourceBundle

/**
 * 读取 application.properties 配置文件
 */
class GlobalConfigUtils {
  private val bundle = ResourceBundle.getBundle("application")

  def heartColumnFamily: String = "MM"

  def getProp(key: String): String = {
    try {
      bundle.getString(key)
    } catch {
      case _: Exception => null
    }
  }
}

object GlobalConfigUtils extends GlobalConfigUtils {
  def main(args: Array[String]): Unit = {
    val str = getProp("spark.worker.timeout")
    println(str)
  }
}







