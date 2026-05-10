package com.travel.spark

import com.travel.bean.{DriverInfo, Opt_alliance_business, OrderInfo, RegisterUsers}
import com.travel.listener.SparkSessionListener
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkEngine {

  def getSparkConf(): SparkConf = {
    val sparkConf: SparkConf = new SparkConf()

      // ========== 1. 内存配置（适配4g虚拟机）==========
      .set("spark.driver.memory", "1024m")
      .set("spark.executor.memory", "1024m")
      .set("spark.executor.cores", "1")
      .set("spark.executor.instances", "2")
      .set("spark.cores.max", "2")

      // ========== 2. 序列化 ==========
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      // ========== 3. 流处理核心配置 ==========
      .set("spark.sql.streaming.backpressure.enabled", "true")
      .set("spark.sql.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.sql.shuffle.partitions", "2")
      // 关键：强制使用 Kafka 0.10.2 协议版本，避免版本不兼容问题
      .set("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")

      // ========== 4. 网络超时（避免卡死）==========
      .set("spark.network.timeout", "300s")
      .set("spark.executor.heartbeatInterval", "60s")

      // ========== 5. 应用名称 ==========
      .setAppName("StructuredStreaming")

    // 注册 Kryo 序列化类
    sparkConf.registerKryoClasses(
      Array(
        classOf[OrderInfo],
        classOf[Opt_alliance_business],
        classOf[DriverInfo],
        classOf[RegisterUsers]
      )
    )

    sparkConf
  }

  def getSparkSession(sparkConf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 全局注册 Task 级监听器（所有 Spark 任务都会触发）
    spark.sparkContext.addSparkListener(new SparkSessionListener())
    spark
  }

  def getStreamingSparkSession(appName: String): SparkSession = {
    val sparkConf: SparkConf = getSparkConf()
    sparkConf.setAppName(appName)
    // 流处理必须关闭动态分配
    sparkConf.set("spark.dynamicAllocation.enabled", "false")
    getSparkSession(sparkConf)
  }

  def getBatchSparkSession(appName: String): SparkSession = {
    val sparkConf: SparkConf = getSparkConf()
    sparkConf.setAppName(appName)
    // 批处理可以开启动态分配
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    // 可选：设置最小/最大 Executor 数
    sparkConf.set("spark.dynamicAllocation.minExecutors", "1")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "4")
    getSparkSession(sparkConf)
  }
}