package com.travel.programApp

import com.travel.spark.SparkEngine
import com.travel.transaction.{DriverTransation, HotAreaOrder, HotOrderTransation, OrderTransation, RenterTransation}
import com.travel.utils.{GlobalConfigUtils, HbaseTools}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.log4j.{Level, Logger}

object SparkSQLHBaseSource {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 使用 SparkEngine 统一创建（自动添加监控）
    val spark = SparkEngine.getBatchSparkSession("SparkSQLHBaseSource")
//loadDriverInfoData才需要传入hconf
//    val hconf: Configuration = HBaseConfiguration.create()
//    hconf.set("hbase.zookeeper.quorum", "hadoop2429:2181,hadoop2428:2181,hadoop2427:2181")
//    hconf.set("hbase.zookeeper.property.clientPort", "2181")

    println("========== 读取 HBase 数据 ==========")

    println("读取 order_info 表...")
    val orderDF = spark.read
      .format("hbase-read")
      .options(Map(
        GlobalConfigUtils.getProp("sparksql_table_schema") -> GlobalConfigUtils.getProp("order.sparksql_table_schema"),
        GlobalConfigUtils.getProp("hbase_table_name") -> GlobalConfigUtils.getProp("syn.table.order_info"),
        GlobalConfigUtils.getProp("hbase_table_schema") -> GlobalConfigUtils.getProp("order.hbase_table_schema")
      )).load()
      .coalesce(2)

    println(s"订单表数据量: ${orderDF.count()} 条")

    // 用户表
    println("读取 renter_info 表...")
    val renterDF = spark.read
      .format("hbase-read")
      .options(Map(
        GlobalConfigUtils.getProp("sparksql_table_schema") -> GlobalConfigUtils.getProp("registe.sparksql_table_schema"),
        GlobalConfigUtils.getProp("hbase_table_name") -> GlobalConfigUtils.getProp("syn.table.renter_info"),
        GlobalConfigUtils.getProp("hbase_table_schema") -> GlobalConfigUtils.getProp("registe.hbase_table_schema")
      )).load()
      .coalesce(2)

    println(s"用户表数据量: ${renterDF.count()} 条")

    // 司机表
    println("读取 driver_info 表...")
    //val driverDF = HbaseTools.loadDriverInfoData(spark, hconf)

    val driverDF = spark.read
      .format("hbase-read")
      .options(Map(
        GlobalConfigUtils.getProp("sparksql_table_schema") -> GlobalConfigUtils.getProp("drivers.spark_sql_table_schema"),
        GlobalConfigUtils.getProp("hbase_table_name") -> GlobalConfigUtils.getProp("syn.table.driver_info"),
        GlobalConfigUtils.getProp("hbase_table_schema") -> GlobalConfigUtils.getProp("driver.hbase_table_schema")
      )).load()
      .coalesce(2)

    println(s"司机表数据量: ${driverDF.count()} 条")

    //注册
    orderDF.createOrReplaceTempView("order")
    renterDF.createOrReplaceTempView("renter")
    driverDF.createOrReplaceTempView("driver")

    // 缓存表
    spark.sqlContext.cacheTable("order")
    spark.sqlContext.cacheTable("renter")
    spark.sqlContext.cacheTable("driver")

    // 订单统计
    try {
      println(">>> 1. 订单监控统计")
      OrderTransation.init(spark)
    } catch {
      case e: Exception =>
        println(s"订单监控统计失败: ${e.getMessage}")
        e.printStackTrace()
    }




    // 司机统计
    try {
      println(">>> 3. 司机统计")
      DriverTransation.init(spark)
    } catch {
      case e: Exception =>
        println(s"司机统计失败: ${e.getMessage}")
        e.printStackTrace()
    }

    // 用户统计
    try {
      println(">>> 2. 用户统计")
      RenterTransation.init(spark)
    } catch {
      case e: Exception =>
        println(s"用户统计失败: ${e.getMessage}")
        e.printStackTrace()
    }

    spark.sqlContext.uncacheTable("order")
    spark.sqlContext.uncacheTable("driver")
    spark.sqlContext.uncacheTable("renter")

    spark.stop()
    HbaseTools.closeHbaseConn()
    println("========== 统计任务完成 ==========")
  }

  //HotOrderTransation.init(spark)

  //   HotAreaOrder.init(sparkSession)
}