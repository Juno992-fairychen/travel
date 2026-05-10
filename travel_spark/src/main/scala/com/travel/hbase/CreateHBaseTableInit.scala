package com.travel.hbase

import com.travel.common.Constants
import com.travel.utils.HbaseTools
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes

object CreateHBaseTableInit {

  def main(args: Array[String]): Unit = {
    val tableNames = Array("order_info", "renter_info", "driver_info", "opt_alliance_business")
    val hbaseConn: Connection = HbaseTools.getHbaseConn
    if (hbaseConn == null) {
      println("获取 HBase 连接失败")
      return
    }

    val admin: Admin = hbaseConn.getAdmin

    // 生成预分区键
    val splitKeys = new Array[Array[Byte]](8)
    for (i <- 0 until 8) {
      val leftPad = StringUtils.leftPad(i.toString, 4, "0") // 0000, 0001, 0002, 0003
      splitKeys(i) = Bytes.toBytes(leftPad + "|")
    }

    try {
      for (tableName <- tableNames) {
        val tn = TableName.valueOf(tableName)
        if (!admin.tableExists(tn)) {
          // 使用新 API 创建表
          val tableBuilder = TableDescriptorBuilder.newBuilder(tn)
          val cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Constants.DEFAULT_DB_FAMILY.getBytes())
          val columnFamily = cfBuilder.build()
          tableBuilder.setColumnFamily(columnFamily)
          val tableDescriptor = tableBuilder.build()

          admin.createTable(tableDescriptor, splitKeys)
          println(s"表 $tableName 创建成功（预分区数: ${splitKeys.length + 1}）")
        } else {
          println(s"表 $tableName 已存在")
        }
      }
    } catch {
      case e: Exception =>
        println(s"创建表失败: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      admin.close()
      hbaseConn.close()
    }
  }
}