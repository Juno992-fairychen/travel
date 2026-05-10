package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants}
import com.travel.listener.StructuredStreamingListener
import com.travel.spark.SparkEngine
import com.travel.utils.{HbaseTools, JsonParse}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.ForeachWriter

object StreamingMaxwellKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(Constants.VEHICLE)
    val topicPattern = topics.mkString("|")
    val checkpointPath = "/user/juno/checkpoint/maxwell_streaming"

    // 1. 复用 SparkEngine（流处理版本）
    val spark = SparkEngine.getStreamingSparkSession("StreamingMaxwellToHBase")

    // 读取 Kafka 流
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribePattern", topicPattern)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as value")

    // 自定义 ForeachWriter
    val writer = new ForeachWriter[Row] {
      var connection: org.apache.hadoop.hbase.client.Connection = _
      var batchCount: Long = 0L

      override def open(partitionId: Long, epochId: Long): Boolean = {
        connection = HbaseTools.getHbaseConn
        println(s"=== [Open] partition=$partitionId, epoch=$epochId, connection=${connection != null}")
        connection != null
      }

      override def process(value: Row): Unit = {
        batchCount += 1
        if (batchCount % 100 == 0) {
          println(s"=== [Process] batchCount=$batchCount")
        }
        try {
          val jsonStr = value.getString(0)
          if (batchCount <= 5) {
            println(s"=== [Process] JSON: $jsonStr")
          }
          val parseResult = JsonParse.parse(jsonStr)
          if (batchCount <= 5) {
            println(s"=== [Process] Parsed: table=${parseResult._1}, type=${parseResult._2.getClass}")
          }
          if (parseResult._1 != "#A") {
            HbaseTools.saveBusinessDatas(parseResult._1, parseResult, connection)
            if (batchCount <= 5) {
              println(s"=== [Process] Saved to HBase: ${parseResult._1}")
            }
          } else {
            println(s"=== [Process] Skipped invalid message")
          }
        } catch {
          case e: Exception =>
            println(s"=== [Process] Error: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      override def close(errorOrNull: Throwable): Unit = {
        if (connection != null && !connection.isClosed) {
          connection.close()
        }
      }
    }

    // 使用 foreach
    val query = kafkaDF
      .writeStream
      .foreach(writer)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", checkpointPath)
      .start()

    // 2. 添加流监控（使用你已有的 StructuredStreamingListener）
    StructuredStreamingListener.register(query, duration = 30)
    query.awaitTermination()
  }
}