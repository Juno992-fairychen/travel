package com.travel.listener

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature
import com.travel.common.JedisUtil
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable

class SparkSessionListener extends SparkListener {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    var jedis: Jedis = null
    try {
      val pool: JedisPool = JedisUtil.getJedisPool
      jedis = pool.getResource

      val metrics: TaskMetrics = taskEnd.taskMetrics
      val taskInfo = taskEnd.taskInfo

      // 任务基本信息
      val taskInfoMap = mutable.Map[String, Any](
        "taskId" -> taskInfo.taskId,
        "stageId" -> taskEnd.stageId,
        "stageAttemptId" -> taskEnd.stageAttemptId,
        "taskType" -> taskEnd.taskType,
        "host" -> taskInfo.host,
        "executorId" -> taskInfo.executorId,
        "speculative" -> taskInfo.speculative,
        "failed" -> taskInfo.failed,
        "killed" -> taskInfo.killed,
        "successful" -> taskInfo.successful,
        "duration" -> (taskInfo.finishTime - taskInfo.launchTime),
        "launchTime" -> taskInfo.launchTime,
        "finishTime" -> taskInfo.finishTime
      )

      // 任务执行指标
      val taskMetricsMap = mutable.Map[String, Any](
        "executorCpuTime" -> metrics.executorCpuTime,
        "executorRunTime" -> metrics.executorRunTime,
        "executorDeserializeTime" -> metrics.executorDeserializeTime,
        "executorDeserializeCpuTime" -> metrics.executorDeserializeCpuTime,
        "resultSize" -> metrics.resultSize,
        "jvmGCTime" -> metrics.jvmGCTime,
        "resultSerializationTime" -> metrics.resultSerializationTime,
        "memoryBytesSpilled" -> metrics.memoryBytesSpilled,
        "diskBytesSpilled" -> metrics.diskBytesSpilled,
        "peakExecutionMemory" -> metrics.peakExecutionMemory
      )

      // Shuffle 读指标
      val shuffleRead = metrics.shuffleReadMetrics
      val shuffleReadMap = mutable.Map[String, Any](
        "remoteBlocksFetched" -> shuffleRead.remoteBlocksFetched,
        "localBlocksFetched" -> shuffleRead.localBlocksFetched,
        "remoteBytesRead" -> shuffleRead.remoteBytesRead,
        "localBytesRead" -> shuffleRead.localBytesRead,
        "fetchWaitTime" -> shuffleRead.fetchWaitTime,
        "recordsRead" -> shuffleRead.recordsRead,
        "totalBlocksFetched" -> shuffleRead.totalBlocksFetched
      )

      // Shuffle 写指标
      val shuffleWrite = metrics.shuffleWriteMetrics
      val shuffleWriteMap = mutable.Map[String, Any](
        "bytesWritten" -> shuffleWrite.bytesWritten,
        "recordsWritten" -> shuffleWrite.recordsWritten,
        "writeTime" -> shuffleWrite.writeTime
      )

      // 输入/输出指标
      val inputMetrics = metrics.inputMetrics
      val outputMetrics = metrics.outputMetrics
      val ioMap = mutable.Map[String, Any](
        "inputBytesRead" -> inputMetrics.bytesRead,
        "inputRecordsRead" -> inputMetrics.recordsRead,
        "outputBytesWritten" -> outputMetrics.bytesWritten,
        "outputRecordsWritten" -> outputMetrics.recordsWritten
      )

      // 合并所有指标
      val allMetrics = mutable.Map[String, Any](
        "taskInfo" -> taskInfoMap,
        "taskMetrics" -> taskMetricsMap,
        "shuffleRead" -> shuffleReadMap,
        "shuffleWrite" -> shuffleWriteMap,
        "inputOutput" -> ioMap
      )

      // 存入 Redis（按 stage 和 task 区分 key）
      val key = s"task_metrics:stage_${taskEnd.stageId}:task_${taskInfo.taskId}"
      val jsonStr = mapper.writeValueAsString(allMetrics)
      jedis.set(key, jsonStr)
      jedis.expire(key, 3600)  // 1 小时过期

      // 同时更新最新指标
      jedis.set("task_metrics_latest", jsonStr)
      jedis.expire("task_metrics_latest", 3600)

    } catch {
      case e: Exception =>
        println(s"[SparkSessionListener] 保存指标失败: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (jedis != null) jedis.close()
    }
  }
}