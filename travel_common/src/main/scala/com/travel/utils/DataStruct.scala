package com.travel.utils

import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Created by ChenJun
 */
object DataStruct {

  // 定义全局的 ObjectMapper 并注册 Scala 模块
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * 转 JSON (返回 Jackson 的 ObjectNode)
   * 注意：Jackson 的 ObjectNode 类似于 Fastjson 的 JSONObject
   */
  def convertJson(tuples: (String, Any)*): ObjectNode = {
    val rootNode = mapper.createObjectNode()
    tuples.foldLeft(rootNode) {
      case (obj, (k, v)) =>
        // 根据 v 的类型放入节点，Jackson 需要区分类型
        v match {
          case value: String  => obj.put(k, value)
          case value: Int     => obj.put(k, value)
          case value: Long    => obj.put(k, value)
          case value: Double  => obj.put(k, value)
          case value: Boolean => obj.put(k, value)
          case _              => obj.put(k, v.toString) // 其他类型转字符串
        }
        obj
    }
  }

  /** 转 map */
  def convertMap(tuples: (String, String)*): java.util.HashMap[String, String] = {
    tuples.foldLeft(new util.HashMap[String, String]()) {
      case (map, (k, v)) =>
        map.put(k, v)
        map
    }
  }

  /** 转 properties */
  def convertProp(tuples: (String, String)*): Properties = {
    tuples.foldLeft(new Properties()) {
      case (prop, (k, v)) =>
        prop.setProperty(k, v)
        prop
    }
  }
}
