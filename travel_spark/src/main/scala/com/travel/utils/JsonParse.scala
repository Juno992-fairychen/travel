package com.travel.utils

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{NullNode, ObjectNode}
import com.travel.bean.{DriverInfo, Opt_alliance_business, OrderInfo, RegisterUsers}

/**
 * JSON 解析工具类（使用 Jackson）
 */
object JsonParse {

  private val objectMapper = new ObjectMapper()

  val order_info_table = GlobalConfigUtils.getProp("syn.table.order_info")
  val renter_info_table = GlobalConfigUtils.getProp("syn.table.renter_info")
  val driver_info_table = GlobalConfigUtils.getProp("syn.table.driver_info")
  val opt_alliance_business = GlobalConfigUtils.getProp("syn.table.opt_alliance_business")

  /**
   * 安全获取 JsonNode 的字符串值
   */
  private def getStringValue(node: JsonNode, fieldName: String): String = {
    val fieldNode = node.get(fieldName)
    if (fieldNode != null && !fieldNode.isNull) fieldNode.asText() else ""
  }

  /**
   * 安全获取 JsonNode 的字符串值（带默认值）
   */
  private def getStringValue(node: JsonNode, fieldName: String, defaultValue: String): String = {
    val fieldNode = node.get(fieldName)
    if (fieldNode != null && !fieldNode.isNull) fieldNode.asText() else defaultValue
  }

  def parse(str: String): (String, Any) = {
    println(s"=== JsonParse.parse: str=$str")
    val json: JsonNode = objectMapper.readTree(str)

    val table = getStringValue(json, "table")
    val type_ = getStringValue(json, "type", "insert")
    val dataNode = json.get("data")

    val bean: (String, Any) = table match {
      // 以 order_info 为开头
      case t if t.startsWith(order_info_table) =>
        val id = getStringValue(dataNode, "id_")
        val create_time = getStringValue(dataNode, "create_time")
        val pay_all = getStringValue(dataNode, "pay_all")
        val pay_normal = getStringValue(dataNode, "pay_normal")
        val open_lng = getStringValue(dataNode, "open_lng")
        val open_lat = getStringValue(dataNode, "open_lat")
        val begin_address_code = getStringValue(dataNode, "begin_address_code")
        val charge_mileage = getStringValue(dataNode, "charge_mileage")
        val city_name = getStringValue(dataNode, "city_name")
        val vehicle_license = getStringValue(dataNode, "vehicle_license")
        val driver_id = getStringValue(dataNode, "driver_id")
        val driver_name = getStringValue(dataNode, "driver_name")
        val cancel = getStringValue(dataNode, "cancel")
        val state = getStringValue(dataNode, "state")
        val order_type = getStringValue(dataNode, "order_type")
        val work_flow = getStringValue(dataNode, "work_flow")
        val close_gps_time = getStringValue(dataNode, "close_gps_time")
        val cancel_time = getStringValue(dataNode, "cancel_time")
        val pay_time = getStringValue(dataNode, "pay_time")

        (table, OrderInfo(
          table,
          id,
          type_,
          pay_all,
          pay_normal,
          create_time,
          open_lng,
          open_lat,
          begin_address_code,
          charge_mileage,
          city_name,
          vehicle_license,
          driver_id,
          driver_name,
          cancel,
          state,
          order_type,
          work_flow,
          close_gps_time,
          cancel_time,
          pay_time
        ))

      case t if t.equals(renter_info_table) || t == renter_info_table =>
        val id = getStringValue(dataNode, "id_")
        val create_time = getStringValue(dataNode, "create_time")
        val last_login_city = getStringValue(dataNode, "last_login_city")
        val last_logon_time = getStringValue(dataNode, "last_logon_time")
        val city_name = getStringValue(dataNode, "city_name")

        (table, RegisterUsers(
          table,
          id,
          type_,
          create_time,
          last_login_city,
          last_logon_time,
          city_name
        ))

      case t if t.equals(driver_info_table) || t == driver_info_table =>
        val id = getStringValue(dataNode, "id_")
        val create_time = getStringValue(dataNode, "create_time")
        val register_city = getStringValue(dataNode, "register_city")
        val driver_name = getStringValue(dataNode, "driver_name")
        val mobile = getStringValue(dataNode, "mobile")
        val driver_type = getStringValue(dataNode, "driver_type")
        val cancel_count = getStringValue(dataNode, "cancel_count")
        val driver_management_id = getStringValue(dataNode, "driver_management_id")

        (table, DriverInfo(
          table,
          id,
          type_,
          create_time,
          register_city,
          driver_name,
          mobile,
          driver_type,
          cancel_count,
          driver_management_id
        ))

      // 加盟表
      case t if t.equals(opt_alliance_business) || t == opt_alliance_business =>
        val id_ = getStringValue(dataNode, "id_")
        val alliance_name = getStringValue(dataNode, "alliance_name")
        val organization_code = getStringValue(dataNode, "organization_code")
        val alliance_role = getStringValue(dataNode, "alliance_role")
        val linkman = getStringValue(dataNode, "linkman")
        val contact_number = getStringValue(dataNode, "contact_number")
        val create_user = getStringValue(dataNode, "create_user")
        val create_time = getStringValue(dataNode, "create_time")
        val update_user = getStringValue(dataNode, "update_user")
        val update_time = getStringValue(dataNode, "update_time")
        val state = getStringValue(dataNode, "state")
        val del_state = getStringValue(dataNode, "del_state")

        (table, Opt_alliance_business(
          table,
          type_,
          id_,
          alliance_name,
          organization_code,
          alliance_role,
          linkman,
          contact_number,
          create_user,
          create_time,
          update_user,
          update_time,
          state,
          del_state
        ))

      case _ =>
        ("#A", "#A")
    }

    bean
  }

  def parseJsonStr(jsonStr: String): Any = {
    val jsonObj: JsonNode = objectMapper.readTree(jsonStr)
    getStringValue(jsonObj, "")
  }
}