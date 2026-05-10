package com.travel.programApp

import com.travel.spark.SparkEngine
import com.travel.utils.HbaseTools
import com.travel.common.{ConfigUtil, Constants, JedisUtil}
import com.travel.listener.{SparkSessionListener, StructuredStreamingListener}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ForeachWriter, Row}

object StreamingKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.ERROR)

    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val chengDuTopic = ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC)
    val haiKouTopic = ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC)
    val topicPattern = s"($chengDuTopic|$haiKouTopic)"
    val checkpointPath = "/user/juno/checkpoint/streaming_gps"

    val spark = SparkEngine.getStreamingSparkSession("StructuredStreamingGPS")

    // 1. 注册 Task 级监控（所有 Spark 任务）
    spark.sparkContext.addSparkListener(new SparkSessionListener())

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      // ========== Kafka 连接配置 ==========
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribePattern", topicPattern)   // 改回 subscribePattern
      // ========== 消费者组配置（使用固定名称，便于排查）==========
      .option("kafka.group.id", "spark-gps-streaming-v2")
      // ========== 消费位点配置 ==========
      .option("startingOffsets", "earliest")      // 从头开始消费
      .option("failOnDataLoss", "false")          // 避免数据丢失导致任务失败
      // ========== Kafka 客户端超时配置（避免卡死）==========
      .option("kafka.session.timeout.ms", "60000")
      .option("kafka.request.timeout.ms", "60000")
      .option("kafka.max.poll.interval.ms", "300000")
      .option("kafka.heartbeat.interval.ms", "5000")
      // ========== 每批次最大拉取量 ==========
      .option("maxOffsetsPerTrigger", "500")
      // ========== 强制指定 Kafka 安全协议 ==========
      .option("kafka.security.protocol", "PLAINTEXT")
      .load()
      .selectExpr("CAST(value AS STRING) as value", "topic")

    val chengduWriter = new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = {
        println(s"[CHENGDU] OPEN: partitionId=$partitionId, epochId=$epochId")
        true
      }
      override def process(value: Row): Unit = {
        val line = value.getString(0)
        println(s"[CHENGDU] PROCESS: ${line.take(50)}...")
        try {
          JedisUtil.saveChengDuJedis(line)
          HbaseTools.saveChengDuGpsData(line)
        } catch {
          case e: Exception => println(s"[CHENGDU] ERROR: ${e.getMessage}")
        }
      }
      override def close(errorOrNull: Throwable): Unit = {
        println(s"[CHENGDU] CLOSE: error=$errorOrNull")
      }
    }

    val haikouWriter = new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = {
        println(s"[HAIKOU] OPEN: partitionId=$partitionId, epochId=$epochId")
        true
      }
      override def process(value: Row): Unit = {
        val line = value.getString(0)
        println(s"[HAIKOU] PROCESS: ${line.take(50)}...")
        try {
          HbaseTools.saveHaiKouOrderData(line)
        } catch {
          case e: Exception => println(s"[HAIKOU] ERROR: ${e.getMessage}")
        }
      }
      override def close(errorOrNull: Throwable): Unit = {
        println(s"[HAIKOU] CLOSE: error=$errorOrNull")
      }
    }

    val chengduQuery = kafkaDF
      .filter($"topic" === chengDuTopic)
      .writeStream
      .foreach(chengduWriter)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", s"$checkpointPath/chengdu")
      .start()

    // 2. 注册 Stream 级监控（流处理专用）
    StructuredStreamingListener.register(chengduQuery, duration = 30)

    val haikouQuery = kafkaDF
      .filter($"topic" === haiKouTopic)
      .writeStream
      .foreach(haikouWriter)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", s"$checkpointPath/haikou")
      .start()

    chengduQuery.awaitTermination()
    haikouQuery.awaitTermination()
  }
}