package com.travel.hbase

import com.travel.common.Constants
import com.travel.utils.HbaseTools

object CreateHTable {
  def main(args: Array[String]): Unit = {
    val tableNames = Array(Constants.HTAB_HAIKOU_ORDER, Constants.HTAB_GPS)
    val families = Array(Constants.DEFAULT_FAMILY)

    // 使用统一方法创建表
    HbaseTools.createTablesIfNotExist(tableNames, families)
  }
}