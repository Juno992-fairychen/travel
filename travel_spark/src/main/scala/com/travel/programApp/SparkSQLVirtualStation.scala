package com.travel.programApp
import java.util
import java.util.Base64
import com.travel.common.{Constants, District}
import com.travel.listener.SparkSessionListener
import com.travel.spark.SparkEngine
import com.travel.utils.{HbaseTools, SparkUtils}
import com.uber.h3core.H3Core
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.jts.geom.{GeometryFactory, Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object SparkSQLVirtualStation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    val spark = SparkEngine.getBatchSparkSession("VirtualStation")
    spark.sparkContext.addSparkListener(new SparkSessionListener())

    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "hadoop2429,hadoop2428,hadoop2427")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.setInt("hbase.client.operation.timeout", 3000)
    val hbaseFrame: DataFrame = HbaseTools.loadHBaseData(spark, hconf)
    //定义成为了一张表
    hbaseFrame.createOrReplaceTempView("order_df")
    //统计所有的虚拟车站

    //封装了虚拟车站的位置求取方法
    val virtual_rdd: RDD[Row] = SparkUtils.getVirtualFrame(spark)
    //求取每一个虚拟车站属于哪一个区，可以获取每一个区域边界的经纬度数据，然后广播出去
    val districtBroadcastVar: Broadcast[util.ArrayList[District]] = SparkUtils.broadCastDistrictValue(spark)
    //根据虚拟车站的经纬度判断属于哪一个区，使用java geometry方式实现
    val finalSaveRow = virtual_rdd.mapPartitions(eachPartitions => {
      val geometryFactory: GeometryFactory = new GeometryFactory()
      val wktReader = new WKTReader(geometryFactory)
      val wktPolygons: mutable.Buffer[(District, Polygon)] = SparkUtils.changeDistictToPolygon(districtBroadcastVar, wktReader)

      eachPartitions.map(row => {
        val lng: String = row.getAs[String]("starting_lng")
        val lat: String = row.getAs[String]("starting_lat")
        val wktPoint = "POINT(" + lng + " " + lat + ")"
        val point:Point = wktReader.read(wktPoint).asInstanceOf[Point]

        val rows: mutable.Buffer[Row] = wktPolygons.map(polygon => {
          if (polygon._2.contains(point)) {
            val fields: Array[Any] = row.toSeq.toArray ++ Seq(polygon._1.getName, 1)
            Row.fromSeq(fields)
          } else {
            null
          }
        }).filter(null != _)
        rows
      })
    })
    val rowRdd: RDD[Row] = finalSaveRow.flatMap(x=>x)
    // 添加：按行政区统计
    val aggregatedRdd: RDD[Row] = rowRdd
      .map(row => (row.getString(row.length - 1), 1))  // (区名, 1)
      .reduceByKey(_ + _)                              // 按区名聚合
      .map { case (district, count) =>
        Row(district, count)                           // 输出 (区名, 数量)
      }
    HbaseTools.saveOrWriteData(hconf, rowRdd, Constants.VIRTUAL_STATION)
    spark.stop()
  }
}
