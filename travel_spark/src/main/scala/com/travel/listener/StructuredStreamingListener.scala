package com.travel.listener

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.travel.common.JedisUtil
import com.travel.utils.TimeUtils
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable

import java.lang.management.{ManagementFactory, GarbageCollectorMXBean}

class StructuredStreamingListener(duration: Int) extends StreamingQueryListener {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private val mapDatas = new mutable.HashMap[String, String]()
  private var finishBatchNum: Int = 0

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    mapDatas.clear()
    mapDatas.put("queryId", event.id.toString)
    mapDatas.put("queryName", event.name)
    mapDatas.put("startTime", event.timestamp.toString)
    // event.timestamp 是 Long 类型
    mapDatas.put("startTimeFormat", TimeUtils.formateDate(event.timestamp, "yyyy-MM-dd HH:mm:ss"))

    saveToRedis("query_started", mapDatas.toMap)
    println(s"[StreamingListener] Stream started: ${event.name}, id: ${event.id}")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val progress = event.progress
    mapDatas.clear()

    // 基本信息
    mapDatas.put("queryId", progress.id.toString)
    mapDatas.put("queryName", progress.name)
    mapDatas.put("batchId", progress.batchId.toString)
    mapDatas.put("batchTimestamp", progress.timestamp.toString)
    // progress.timestamp 是 Long 类型
    mapDatas.put("batchTimestampFormat", TimeUtils.formateDate(progress.timestamp, "yyyy-MM-dd HH:mm:ss"))

    // 输入数据统计
    mapDatas.put("numInputRows", progress.numInputRows.toString)
    mapDatas.put("inputRowsPerSecond", progress.inputRowsPerSecond.toString)
    mapDatas.put("processedRowsPerSecond", progress.processedRowsPerSecond.toString)

    // 处理时间统计
    val durationMsMap = progress.durationMs
    if (durationMsMap != null && !durationMsMap.isEmpty) {
      val totalDuration = durationMsMap.asScala.values.foldLeft(0L)(_ + _)
      mapDatas.put("totalDurationMs", totalDuration.toString)

      mapDatas.put("addBatchMs", Option(durationMsMap.get("addBatch")).map(_.toString).getOrElse("0"))
      mapDatas.put("getBatchMs", Option(durationMsMap.get("getBatch")).map(_.toString).getOrElse("0"))
      mapDatas.put("walCommitMs", Option(durationMsMap.get("walCommit")).map(_.toString).getOrElse("0"))
      mapDatas.put("queryPlanningMs", Option(durationMsMap.get("queryPlanning")).map(_.toString).getOrElse("0"))
      mapDatas.put("offsetLogMs", Option(durationMsMap.get("offsetLog")).map(_.toString).getOrElse("0"))
      mapDatas.put("commitLogMs", Option(durationMsMap.get("commitLog")).map(_.toString).getOrElse("0"))
    }

    // ========== 新增：JVM GC 时间监控 ==========
    try {
      // 获取当前 JVM 的 GC 时间
      val gcTime = getCurrentGCTime()
      mapDatas.put("jvmGCTime", gcTime.toString)

      // 可选：计算 GC 时间占比
      val batchProcessingTime = Option(durationMsMap.get("addBatch")).map(_.asInstanceOf[Long]).getOrElse(1L)
      val gcPercentage = (gcTime.toDouble / batchProcessingTime) * 100
      mapDatas.put("gcPercentage", f"$gcPercentage%.2f")

      if (gcPercentage > 30) {
        mapDatas.put("gcWarning", s"GC时间过高: ${gcPercentage}%")
        println(s"[StreamingListener] GC警告: 时间=${gcTime}ms, 占比=${gcPercentage}%")
      }
    } catch {
      case e: Exception =>
        mapDatas.put("jvmGCTime", "0")
        mapDatas.put("gcPercentage", "0")
        println(s"[StreamingListener] 获取GC时间失败: ${e.getMessage}")
    }

    // 水位线 (watermark) - 修复类型转换问题
    try {
      val eventTimeMap = progress.eventTime
      if (eventTimeMap != null && eventTimeMap.containsKey("watermark")) {
        val watermarkStr = eventTimeMap.get("watermark")
        if (watermarkStr != null && watermarkStr.nonEmpty) {
          // 直接使用 TimeUtils.formateDate，它已经支持 Any 类型和 ISO 字符串
          mapDatas.put("eventTimeWatermarkFormat", TimeUtils.formateDate(watermarkStr, "yyyy-MM-dd HH:mm:ss"))
          mapDatas.put("eventTimeWatermark", watermarkStr)
        } else {
          mapDatas.put("eventTimeWatermark", "0")
        }
      } else {
        mapDatas.put("eventTimeWatermark", "0")
      }
    } catch {
      case e: Exception =>
        println(s"[StreamingListener] Error parsing watermark: ${e.getMessage}")
        mapDatas.put("eventTimeWatermark", "0")
    }

    // Source 信息
    if (progress.sources != null && progress.sources.nonEmpty) {
      val firstSource = progress.sources.head
      mapDatas.put("sourceDescription", firstSource.description)
      mapDatas.put("sourceNumInputRows", firstSource.numInputRows.toString)
      mapDatas.put("sourceProcessedRowsPerSecond", firstSource.processedRowsPerSecond.toString)
    }

    // Sink 信息
    if (progress.sink != null) {
      mapDatas.put("sinkDescription", progress.sink.description)
    }

    // State 信息
    if (progress.stateOperators != null && progress.stateOperators.nonEmpty) {
      val firstState = progress.stateOperators.head
      mapDatas.put("stateNumRowsTotal", firstState.numRowsTotal.toString)
      mapDatas.put("stateNumRowsUpdated", firstState.numRowsUpdated.toString)
      mapDatas.put("stateMemoryUsedBytes", firstState.memoryUsedBytes.toString)
    }

    // 阻塞检测
    val batchProcessingTime = Option(durationMsMap.get("addBatch")).map(_.asInstanceOf[Long]).getOrElse(0L)
    val threshold = duration.toLong * 1000L
    if (threshold < batchProcessingTime) {
      mapDatas.put("warning", s"阻塞！batchId=${progress.batchId}, 处理时间=${batchProcessingTime}ms")
    }

    finishBatchNum += 1
    mapDatas.put("finishBatchNum", finishBatchNum.toString)

    saveToRedis(s"streaming_metrics_${progress.batchId}", mapDatas.toMap)
    saveToRedis("streaming_metrics_latest", mapDatas.toMap)

    println(s"[StreamingListener] Batch ${progress.batchId}: ${progress.numInputRows} rows")
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    mapDatas.clear()
    mapDatas.put("queryId", event.id.toString)
    mapDatas.put("exception", event.exception.getOrElse("none"))
    mapDatas.put("terminatedTime", System.currentTimeMillis().toString)
    mapDatas.put("terminatedTimeFormat", TimeUtils.formateDate(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"))

    saveToRedis("query_terminated", mapDatas.toMap)
    println(s"[StreamingListener] Stream terminated: ${event.id}")
  }

  private def getCurrentGCTime(): Long = {
    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans
    gcBeans.asScala.map(_.getCollectionTime).sum
  }

  private def saveToRedis(key: String, data: Map[String, String]): Unit = {
    var jedis: Jedis = null
    try {
      val pool = JedisUtil.getJedisPool
      jedis = pool.getResource
      val jsonStr = mapper.writeValueAsString(data)
      jedis.set(key, jsonStr)
      jedis.expire(key, 3600)
    } catch {
      case e: Exception => println(s"[StreamingListener] 保存监控数据到 Redis 失败: ${e.getMessage}")
    } finally {
      if (jedis != null) jedis.close()
    }
  }
}

object StructuredStreamingListener {
  def register(query: StreamingQuery, duration: Int = 30): StructuredStreamingListener = {
    val listener = new StructuredStreamingListener(duration)
    query.sparkSession.streams.addListener(listener)
    println(s"[StreamingListener] Structured Streaming 监听器已注册，阻塞阈值: ${duration}s")
    listener
  }
}