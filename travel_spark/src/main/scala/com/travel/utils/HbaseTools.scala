package com.travel.utils

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.JavaConverters._
import java.sql.Timestamp
import java.util.{Base64, Date}
import java.util.regex.Pattern
import com.travel.bean._
import com.travel.common._
import com.travel.loggings.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import redis.clients.jedis.Jedis

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ListBuffer

object HbaseTools extends Logging with Serializable {

  def getHbaseConfiguration(): Configuration = {
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", GlobalConfigUtils.getProp("hbase.zookeeper.quorum"))
    hconf.set("hbase.zookeeper.property.clientPort", GlobalConfigUtils.getProp("hbase.zookeeper.property.clientPort"))
    hconf.setInt("hbase.client.operation.timeout", 3000)
    hconf
  }

  // ==================== HBase 连接管理（单例模式） ====================
  private val connectionRef = new AtomicReference[Connection](null)
  def getHbaseConn: Connection = {
    var conn = connectionRef.get()
    if (conn == null || conn.isClosed) {
      this.synchronized {
        conn = connectionRef.get()
        if (conn == null || conn.isClosed) {
          // 复用上面的 getHbaseConfiguration()
          val config = getHbaseConfiguration()
          conn = ConnectionFactory.createConnection(config)
          connectionRef.set(conn)
          println("[HBase] 创建新连接")
        }
      }
    }
    conn
  }

  def closeHbaseConn(): Unit = {
    val conn = connectionRef.get()
    if (conn != null && !conn.isClosed) {
      conn.close()
      connectionRef.set(null)
      println("[HBase] 关闭连接")
    }
  }

  // ==================== RowKey 生成方法 ====================
  def rowKeyWithHashPrefix(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val hash_prefix = getHashCode(rkString)
    Bytes.add(Bytes.toBytes(hash_prefix), Bytes.toBytes(rkString))
  }

  def rowKeyWithMD5Prefix(separator: String, length: Int, column: String*): Array[Byte] = {
    val columns = column.mkString(separator)
    var md5_prefix = MD5Hash.getMD5AsHex(Bytes.toBytes(columns))
    if (length < 8) {
      md5_prefix = md5_prefix.substring(0, 8)
    } else if (length <= 32) {
      md5_prefix = md5_prefix.substring(0, length)
    } else {
      md5_prefix
    }
    Bytes.toBytes(Array(md5_prefix, columns).mkString(separator))
  }

  def rowKey(column: String*): Array[Byte] = Bytes.toBytes(column.mkString(""))

  private def getHashCode(field: String): Short = {
    (field.hashCode() & 0x7FFF).toShort
  }

  // ==================== HBase 表操作 ====================
  def createTablesIfNotExist(tableNames: Array[String], families: Array[String]): Unit = {
    val connection = getHbaseConn
    if (connection == null) return

    val admin = connection.getAdmin
    try {
      for (tableName <- tableNames) {
        val tn = TableName.valueOf(tableName)
        if (!admin.tableExists(tn)) {
          val tableBuilder = TableDescriptorBuilder.newBuilder(tn)
          for (family <- families) {
            val cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes())
            cfBuilder.setMaxVersions(1)
            cfBuilder.setBloomFilterType(BloomType.ROW)
            tableBuilder.setColumnFamily(cfBuilder.build())
          }
          admin.createTable(tableBuilder.build())
          println(s"表 $tableName 创建成功")
        } else {
          println(s"表 $tableName 已存在")
        }
      }
    } finally {
      admin.close()
      connection.close()
    }
  }

  def createTableWithSplits(tablename: String, regionNum: Int, families: Array[String]): Unit = {
    val connection = getHbaseConn
    if (connection == null) return

    this.synchronized {
      val hexsplit = new HexStringSplit()
      val splitkeys = hexsplit.split(regionNum)
      val admin = connection.getAdmin
      val tableName = TableName.valueOf(tablename)

      try {
        if (!admin.tableExists(tableName)) {
          val tableBuilder = TableDescriptorBuilder.newBuilder(tableName)
          families.foreach { family =>
            val cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes())
            cfBuilder.setMaxVersions(1)
            cfBuilder.setBloomFilterType(BloomType.ROW)
            tableBuilder.setColumnFamily(cfBuilder.build())
          }
          admin.createTable(tableBuilder.build(), splitkeys)
          println(s"表 $tablename 创建成功，预分区数: ${splitkeys.length + 1}")
        }
      } finally {
        admin.close()
        connection.close()
      }
    }
  }

  private val tableExistsCache = mutable.Set[String]()

  private def ensureTableExists(conn: Connection, tableName: String, family: String): Unit = {
    if (!tableExistsCache.contains(tableName)) {
      this.synchronized {
        if (!tableExistsCache.contains(tableName)) {
          createTablesIfNotExist(Array(tableName), Array(family))
          tableExistsCache.add(tableName)
        }
      }
    }
  }

  def putMapData(conn: Connection, tableName: String, rowKey: String, mapData: Map[String, Any]): Unit = {
    println(s"=== putMapData ENTER: table=$tableName, rowKey=$rowKey, mapData.size=${mapData.size}")
    ensureTableExists(conn, tableName, Constants.DEFAULT_DB_FAMILY)
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val rowkey = Bytes.toBytes(RowkeyUtil.getRowKey(rowKey, Constants.DEFAULT_REGION_NUM))
      val put = new Put(rowkey)
      println(s"=== putMapData: rowkey bytes=${Bytes.toString(rowkey)}")
      mapData.foreach { case (k, v) =>
        put.addColumn(
          Bytes.toBytes(Constants.DEFAULT_DB_FAMILY),
          Bytes.toBytes(k),
          Bytes.toBytes(Option(v).map(_.toString).getOrElse(""))
        )
      }
      table.put(put)
      println(s"=== putMapData SUCCESS: table=$tableName, rowKey=$rowKey")
    } catch {
      case e: Exception =>
        println(s"=== putMapData ERROR: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      table.close()
    }
  }

  def putBatchData(conn: Connection, tableName: String, dataList: List[(String, Map[String, Any])]): Unit = {
    if (dataList.isEmpty) return

    ensureTableExists(conn, tableName, Constants.DEFAULT_DB_FAMILY)

    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val puts = dataList.map { case (rowKey, mapData) =>
        val rowkey = Bytes.toBytes(RowkeyUtil.getRowKey(rowKey, Constants.DEFAULT_REGION_NUM))
        val put = new Put(rowkey)
        mapData.foreach { case (k, v) =>
          put.addColumn(
            Bytes.toBytes(Constants.DEFAULT_DB_FAMILY),
            Bytes.toBytes(k),
            Bytes.toBytes(Option(v).map(_.toString).getOrElse(""))
          )
        }
        put
      }
      table.put(puts.asJava)
    } finally {
      table.close()
    }
  }

  def delete_hdfspath(url: String): Unit = {
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path: Path = new Path(url)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }

  // ==================== Structured Streaming 相关 ====================

  def createKafkaStream(spark: SparkSession, kafkaParams: Map[String, String], topics: Array[String]): DataFrame = {
    val topicPattern = topics.mkString("|")
    spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .option("subscribePattern", topicPattern)
      .option("failOnDataLoss", "false")
      .load()
  }

  case class ChengDuGps(driverId: String, orderId: String, timestamp: Long, lng: Double, lat: Double, isEnd: Boolean)

  case class HaiKouOrderData(orderId: String, productId: String, cityId: String, district: String, county: String,
                             `type`: String, comboType: String, trafficType: String, passengerCount: String,
                             driverProductId: String, startDestDistance: String, arriveTime: String, departureTime: String,
                             preTotalFee: String, normalTime: String, bubbleTraceId: String, product1level: String,
                             destLng: String, destLat: String, startingLng: String, startingLat: String,
                             year: String, month: String, day: String)

  def parseChengDuGps(line: String): Option[ChengDuGps] = {
    try {
      val parts = line.split(",")
      if (parts.length >= 5) {
        val isEnd = line.contains("end")
        Some(ChengDuGps(
          driverId = parts(0),
          orderId = parts(1),
          timestamp = parts(2).toLong,
          lng = parts(3).toDouble,
          lat = parts(4).toDouble,
          isEnd = isEnd
        ))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  def parseHaiKouOrder(line: String): Option[HaiKouOrderData] = {
    try {
      val fields = line.split("\t")
      if (fields.length == 24 && !line.contains("dwv_order_make_haikou")) {
        Some(HaiKouOrderData(
          orderId = fields(0), productId = fields(1), cityId = fields(2), district = fields(3), county = fields(4),
          `type` = fields(5), comboType = fields(6), trafficType = fields(7), passengerCount = fields(8),
          driverProductId = fields(9), startDestDistance = fields(10), arriveTime = fields(11),
          departureTime = fields(12), preTotalFee = fields(13), normalTime = fields(14),
          bubbleTraceId = fields(15), product1level = fields(16), destLng = fields(17), destLat = fields(18),
          startingLng = fields(19), startingLat = fields(20), year = fields(21), month = fields(22), day = fields(23)
        ))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  class ChengDuGpsForeachWriter(connection: Connection) extends ForeachWriter[ChengDuGps] {
    override def open(partitionId: Long, epochId: Long): Boolean = true

    override def process(value: ChengDuGps): Unit = {
      val rowkey = value.orderId + "_" + value.timestamp
      val put = new Put(rowkey.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "CITYCODE".getBytes(), Constants.CITY_CODE_CHENG_DU.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DRIVERID".getBytes(), value.driverId.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "ORDERID".getBytes(), value.orderId.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "TIMESTAMP".getBytes(), value.timestamp.toString.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "TIME".getBytes(),
        DateUtils.formateDate(new Date((value.timestamp + "000").toLong), "yyyy-MM-dd HH:mm:ss").getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "LNG".getBytes(), value.lng.toString.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "LAT".getBytes(), value.lat.toString.getBytes())

      val table = connection.getTable(TableName.valueOf(Constants.HTAB_GPS))
      try {
        table.put(put)
      } finally {
        table.close()
      }

      if (!value.isEnd) {
        JedisUtil.saveChengDuJedis(s"${value.driverId},${value.orderId},${value.timestamp},${value.lng},${value.lat}")
      }
    }

    override def close(errorOrNull: Throwable): Unit = {}
  }

  class HaiKouOrderForeachWriter(connection: Connection) extends ForeachWriter[HaiKouOrderData] {
    override def open(partitionId: Long, epochId: Long): Boolean = true

    override def process(value: HaiKouOrderData): Unit = {
      val rowkey = value.orderId + "_" + value.departureTime.replaceAll("-", "") + value.arriveTime.replaceAll(":", "")
      val put = new Put(rowkey.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "ORDER_ID".getBytes(), value.orderId.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PRODUCT_ID".getBytes(), value.productId.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "CITY_ID".getBytes(), value.cityId.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DISTRICT".getBytes(), value.district.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "COUNTY".getBytes(), value.county.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "TYPE".getBytes(), value.`type`.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "COMBO_TYPE".getBytes(), value.comboType.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "TRAFFIC_TYPE".getBytes(), value.trafficType.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PASSENGER_COUNT".getBytes(), value.passengerCount.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DRIVER_PRODUCT_ID".getBytes(), value.driverProductId.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "START_DEST_DISTANCE".getBytes(), value.startDestDistance.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "ARRIVE_TIME".getBytes(), value.arriveTime.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DEPARTURE_TIME".getBytes(), value.departureTime.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PRE_TOTAL_FEE".getBytes(), value.preTotalFee.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "NORMAL_TIME".getBytes(), value.normalTime.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "BUBBLE_TRACE_ID".getBytes(), value.bubbleTraceId.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PRODUCT_1LEVEL".getBytes(), value.product1level.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DEST_LNG".getBytes(), value.destLng.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DEST_LAT".getBytes(), value.destLat.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "STARTING_LNG".getBytes(), value.startingLng.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "STARTING_LAT".getBytes(), value.startingLat.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "YEAR".getBytes(), value.year.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "MONTH".getBytes(), value.month.getBytes())
      put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DAY".getBytes(), value.day.getBytes())

      val table = connection.getTable(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))
      try {
        table.put(put)
      } finally {
        table.close()
      }
    }

    override def close(errorOrNull: Throwable): Unit = {}
  }

  def startStreaming(
                      spark: SparkSession,
                      kafkaParams: Map[String, String],
                      topics: Array[String],
                      checkpointPath: String
                    ): StreamingQuery = {

    import spark.implicits._

    val stream = createKafkaStream(spark, kafkaParams, topics)

    val parsedStream = stream.selectExpr("CAST(value AS STRING) as value")
      .as[String]
      .flatMap { line =>
        if (line.split(",").length > 4) {
          parseChengDuGps(line).map(gps => ("chengdu", gps))
        } else if (line.contains("\t")) {
          parseHaiKouOrder(line).map(order => ("haikou", order))
        } else {
          None
        }
      }

    val chengduStream = parsedStream.filter(_._1 == "chengdu").map(_._2.asInstanceOf[ChengDuGps])
    val haikouStream = parsedStream.filter(_._1 == "haikou").map(_._2.asInstanceOf[HaiKouOrderData])

    val chengduQuery = chengduStream.writeStream
      .foreach(new ChengDuGpsForeachWriter(getHbaseConn))
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", checkpointPath + "/chengdu")
      .start()

    val haikouQuery = haikouStream.writeStream
      .foreach(new HaiKouOrderForeachWriter(getHbaseConn))
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", checkpointPath + "/haikou")
      .start()

    chengduQuery
  }

  def saveBatchData(chengduListBuffer: ListBuffer[Put], CHENG_DU_GPS_TOPIC: String): Unit = {
    import scala.collection.JavaConverters._
    HBaseUtil.savePuts(chengduListBuffer.asJava, CHENG_DU_GPS_TOPIC)
  }

  def getOffsetFromHBase(connection: Connection, admin: Admin, topics: Array[String], group: String): Map[TopicPartition, Long] = {
    Map.empty
  }

  def saveBatchOffset(group: String, topic: String, partition: String, offset: Long): Unit = {}

  val columnFamily = GlobalConfigUtils.heartColumnFamily
  val order_info_table = GlobalConfigUtils.getProp("syn.table.order_info")
  val renter_info_table = GlobalConfigUtils.getProp("syn.table.renter_info")
  val driver_info_table = GlobalConfigUtils.getProp("syn.table.driver_info")
  val opt_alliance_business = GlobalConfigUtils.getProp("syn.table.opt_alliance_business")

  def saveBusinessDatas(interpreter: String, parse: (String, Any), conn: Connection): Unit = {
    println(s"=== saveBusinessDatas: interpreter=$interpreter")

    interpreter match {
      case t if t.contains("order_info") =>
        val data = parse._2.asInstanceOf[OrderInfo]
        if (data.id != null) {
          val mapData = ReflectBean.reflect(data, "order_info")
          val rowkey = RowkeyUtil.getRowKey(data.id, Constants.DEFAULT_REGION_NUM)
          putMapData(conn, "order_info", rowkey, mapData)
        }

      case t if t.contains("renter_info") =>
        val data = parse._2.asInstanceOf[RegisterUsers]
        val mapData = ReflectBean.reflect(data, "renter_info")
        val rowkey = RowkeyUtil.getRowKey(data.id.toString, Constants.DEFAULT_REGION_NUM)
        putMapData(conn, "renter_info", rowkey, mapData)

      case t if t.contains("driver_info") =>
        val data = parse._2.asInstanceOf[DriverInfo]
        val mapData = ReflectBean.reflect(data, "driver_info")
        val rowkey = RowkeyUtil.getRowKey(data.id.toString, Constants.DEFAULT_REGION_NUM)
        putMapData(conn, "driver_info", rowkey, mapData)

      case t if t.contains("opt_alliance") =>
        val data = parse._2.asInstanceOf[Opt_alliance_business]
        val mapData = ReflectBean.reflect(data, "opt_alliance_business")
        val rowkey = RowkeyUtil.getRowKey(data.id_, Constants.DEFAULT_REGION_NUM)
        putMapData(conn, "opt_alliance_business", rowkey, mapData)

      case _ =>
        println(s"=== NO MATCH: $interpreter")
    }
  }

  def saveChengDuGpsData(line: String): Unit = {
    val connection = getHbaseConn
    if (connection == null) return

    try {
      val parts = line.split(",")
      if (parts.length >= 5 && !line.contains("end")) {
        val driverId = parts(0)
        val orderId = parts(1)
        val timestamp = parts(2).toLong
        val lng = parts(3)
        val lat = parts(4)
        val rowkey = orderId + "_" + timestamp

        val put = new Put(rowkey.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "CITYCODE".getBytes(), Constants.CITY_CODE_CHENG_DU.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DRIVERID".getBytes(), driverId.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "ORDERID".getBytes(), orderId.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "TIMESTAMP".getBytes(), timestamp.toString.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "LNG".getBytes(), lng.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "LAT".getBytes(), lat.getBytes())

        val table = connection.getTable(TableName.valueOf(Constants.HTAB_GPS))
        try {
          table.put(put)
        } finally {
          table.close()
        }
      }
    } catch {
      case e: Exception => error(s"保存成都GPS数据失败: ${e.getMessage}")
    } finally {
      connection.close()
    }
  }

  def saveHaiKouOrderData(line: String): Unit = {
    val connection = getHbaseConn
    if (connection == null) return

    try {
      val fields = line.split("\t")
      if (fields.length == 24 && !line.contains("dwv_order_make_haikou")) {
        val rowkey = fields(0) + "_" + fields(13).replaceAll("-", "") + fields(14).replaceAll(":", "")

        val put = new Put(rowkey.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "ORDER_ID".getBytes(), fields(0).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PRODUCT_ID".getBytes(), fields(1).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "CITY_ID".getBytes(), fields(2).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DISTRICT".getBytes(), fields(3).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "COUNTY".getBytes(), fields(4).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "TYPE".getBytes(), fields(5).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "COMBO_TYPE".getBytes(), fields(6).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "TRAFFIC_TYPE".getBytes(), fields(7).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PASSENGER_COUNT".getBytes(), fields(8).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DRIVER_PRODUCT_ID".getBytes(), fields(9).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "START_DEST_DISTANCE".getBytes(), fields(10).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "ARRIVE_TIME".getBytes(), fields(11).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DEPARTURE_TIME".getBytes(), fields(12).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PRE_TOTAL_FEE".getBytes(), fields(13).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "NORMAL_TIME".getBytes(), fields(14).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "BUBBLE_TRACE_ID".getBytes(), fields(15).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "PRODUCT_1LEVEL".getBytes(), fields(16).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DEST_LNG".getBytes(), fields(17).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DEST_LAT".getBytes(), fields(18).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "STARTING_LNG".getBytes(), fields(19).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "STARTING_LAT".getBytes(), fields(20).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "YEAR".getBytes(), fields(21).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "MONTH".getBytes(), fields(22).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(), "DAY".getBytes(), fields(23).getBytes())

        val table = connection.getTable(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))
        try {
          table.put(put)
        } finally {
          table.close()
        }
      }
    } catch {
      case e: Exception => error(s"保存海口订单数据失败: ${e.getMessage}")
    } finally {
      connection.close()
    }
  }

  def loadHBaseData(sparkSession: SparkSession, conf: Configuration): DataFrame = {
    val context = sparkSession.sparkContext
    conf.set(TableInputFormat.INPUT_TABLE, Constants.HTAB_HAIKOU_ORDER)

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(Constants.DEFAULT_FAMILY))
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("ORDER_ID"))
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("CITY_ID"))
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LNG"))
    scan.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LAT"))

    val scanBytes = ProtobufUtil.toScan(scan).toByteArray
    val scanStr = Base64.getEncoder.encodeToString(scanBytes)
    conf.set(TableInputFormat.SCAN, scanStr)

    val rddResult: RDD[(ImmutableBytesWritable, Result)] = context.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    import sparkSession.implicits._

    val haiKouOrderRdd: RDD[HaiKouOrder] = rddResult.mapPartitions { eachPartition =>
      eachPartition.map { case (_, result) =>
        val order_id = Bytes.toString(result.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("ORDER_ID")))
        val city_id = Bytes.toString(result.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("CITY_ID")))
        val starting_lng = Bytes.toString(result.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LNG")))
        val starting_lat = Bytes.toString(result.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LAT")))

        HaiKouOrder(order_id, city_id, starting_lng, starting_lat)
      }
    }

    haiKouOrderRdd.toDF()
  }

  def saveOrWriteData(configuration: Configuration, result: RDD[Row], tableName: String): Unit = {
    configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(configuration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val finalSavePairRdd: RDD[(ImmutableBytesWritable, Put)] = result.mapPartitions { eachPartition =>
      eachPartition.map { eachRow =>
        val orderId = eachRow.getString(0)
        val cityId = eachRow.getString(1)
        val lng = eachRow.getString(2)
        val lat = eachRow.getString(3)
        val stationCount = eachRow.getInt(4)
        val districtName = eachRow.getString(5)

        val put = new Put(Bytes.toBytes(orderId))
        val rowKey = new ImmutableBytesWritable(Bytes.toBytes(orderId))

        put.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("ORDER_ID"), Bytes.toBytes(orderId))
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("CITY_ID"), Bytes.toBytes(cityId))
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LNG"), Bytes.toBytes(lng))
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LAT"), Bytes.toBytes(lat))
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("DISTRICT_NAME"), Bytes.toBytes(districtName))
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STATION_COUNT"), Bytes.toBytes(stationCount))

        (rowKey, put)
      }
    }

    try {
      val connection = HBaseUtil.getConnection
      val admin = connection.getAdmin
      val tn = TableName.valueOf(tableName)

      if (!admin.tableExists(tn)) {
        val tableBuilder = TableDescriptorBuilder.newBuilder(tn)
        val cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Constants.DEFAULT_FAMILY.getBytes())
        cfBuilder.setMaxVersions(1)
        tableBuilder.setColumnFamily(cfBuilder.build())
        admin.createTable(tableBuilder.build())
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"创建HBase表 $tableName 失败", e)
    }

    finalSavePairRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  // ==================== 读取 order_info/driver_info/renter_info 表（离线统计用） ====================

  def loadOrderInfoData(sparkSession: SparkSession, conf: Configuration): DataFrame = {
    val context = sparkSession.sparkContext
    conf.set(TableInputFormat.INPUT_TABLE, "order_info")

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(Constants.DEFAULT_DB_FAMILY))

    val scanBytes = ProtobufUtil.toScan(scan).toByteArray
    val scanStr = Base64.getEncoder.encodeToString(scanBytes)
    conf.set(TableInputFormat.SCAN, scanStr)

    val rddResult = context.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import sparkSession.implicits._

    rddResult.mapPartitions { eachPartition =>
      eachPartition.map { case (_, result) =>
        val id = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("id")))
        val create_time = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("create_time")))
        val pay_all = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("pay_all")))
        val begin_address_code = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("begin_address_code")))
        val city_name = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("city_name")))
        val vehicle_license = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("vehicle_license")))
        val driver_id = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("driver_id")))
        val driver_name = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("driver_name")))
        val cancel = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("cancel")))
        val close_gps_time = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("close_gps_time")))

        OrderInfoRow(id, create_time, pay_all, begin_address_code, city_name,
          vehicle_license, driver_id, driver_name, cancel, close_gps_time)
      }
    }.toDF()
  }

  def loadDriverInfoData(sparkSession: SparkSession, conf: Configuration): DataFrame = {
    val context = sparkSession.sparkContext
    conf.set(TableInputFormat.INPUT_TABLE, "driver_info")

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(Constants.DEFAULT_DB_FAMILY))

    val scanBytes = ProtobufUtil.toScan(scan).toByteArray
    val scanStr = Base64.getEncoder.encodeToString(scanBytes)
    conf.set(TableInputFormat.SCAN, scanStr)

    val rddResult = context.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import sparkSession.implicits._

    rddResult.mapPartitions { eachPartition =>
      eachPartition.map { case (_, result) =>
        val id = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("id")))
        val create_time = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("create_time")))
        val register_city = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("register_city")))
        val driver_name = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("driver_name")))
        val mobile = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("mobile")))
        val driver_type = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("driver_type")))
        val cancel_count = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("cancel_count")))
        val driver_management_id = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("driver_management_id")))

        DriverInfoRow(id, create_time, register_city, driver_name, mobile,
          driver_type, cancel_count, driver_management_id)
      }
    }.toDF()
  }

  def loadRenterInfoData(sparkSession: SparkSession, conf: Configuration): DataFrame = {
    val context = sparkSession.sparkContext
    conf.set(TableInputFormat.INPUT_TABLE, "renter_info")

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(Constants.DEFAULT_DB_FAMILY))

    val scanBytes = ProtobufUtil.toScan(scan).toByteArray
    val scanStr = Base64.getEncoder.encodeToString(scanBytes)
    conf.set(TableInputFormat.SCAN, scanStr)

    val rddResult = context.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import sparkSession.implicits._

    rddResult.mapPartitions { eachPartition =>
      eachPartition.map { case (_, result) =>
        val id = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("id")))
        val create_time = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("create_time")))
        val last_login_city = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("last_login_city")))
        val last_logon_time = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("last_logon_time")))
        val city_name = Bytes.toString(result.getValue(Bytes.toBytes("MM"), Bytes.toBytes("city_name")))

        RenterInfoRow(id, create_time, last_login_city, last_logon_time, city_name)
      }
    }.toDF()
  }

  // ==================== Case Class 定义 ====================
  case class OrderInfoRow(
                           id: String, create_time: String, pay_all: String, begin_address_code: String,
                           city_name: String, vehicle_license: String, driver_id: String, driver_name: String,
                           cancel: String, close_gps_time: String
                         )

  case class DriverInfoRow(
                            id: String, create_time: String, register_city: String, driver_name: String,
                            mobile: String, driver_type: String, cancel_count: String, driver_management_id: String
                          )

  case class RenterInfoRow(
                            id: String, create_time: String, last_login_city: String, last_logon_time: String, city_name: String
                          )
}