package com.travel.utils

import java.util
import java.util.Base64

import com.travel.common.{District, MapUtil}
import com.uber.h3core.H3Core
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.Polygon
import org.locationtech.jts.io.WKTReader

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkUtils {

  val h3 = H3Core.newInstance

  def getVirtualFrame(sparkSession: SparkSession): RDD[Row] = {
    sparkSession.udf.register("locationToH3", new UDF3[String, String, Int, Long]() {
      override def call(lat: String, lng: String, result: Int): Long = {
        h3.geoToH3(lat.toDouble, lng.toDouble, result)
      }
    }, DataTypes.LongType)

    val order_sql =
      s"""
         | select
         |         order_id,
         |         city_id,
         |         starting_lng,
         |         starting_lat,
         |         locationToH3(starting_lat,starting_lng,12) as h3code
         |          from order_df
      """.stripMargin

    val gridDf = sparkSession.sql(order_sql)
    gridDf.createOrReplaceTempView("order_grid")

    val sql: String =
      s"""
         | select
         |   order_id,
         |   city_id,
         |   starting_lng,
         |   starting_lat,
         |   rn
         | from (
         |   select
         |     order_id,
         |     city_id,
         |     starting_lng,
         |     starting_lat,
         |     row_number() over(partition by order_grid.h3code order by starting_lng, starting_lat asc) as rn
         |   from order_grid
         |   join (
         |     select h3code, count(1) as totalResult
         |     from order_grid
         |     group by h3code
         |     having totalResult >= 1
         |   ) groupcount on order_grid.h3code = groupcount.h3code
         | ) t
         | where rn = 1
      """.stripMargin

    val virtual_frame: DataFrame = sparkSession.sql(sql)
    virtual_frame.show(20)

    virtual_frame.rdd
  }

  def broadCastDistrictValue(sparkSession: SparkSession): Broadcast[util.ArrayList[District]] = {
    val districtList = new java.util.ArrayList[District]()
    val districts = MapUtil.getDistricts("海口市", null)
    MapUtil.parseDistrictInfo(districts, null, districtList)
    sparkSession.sparkContext.broadcast(districtList)
  }

  def changeDistictToPolygon(districtsBroadcastVar: Broadcast[util.ArrayList[District]], reader: WKTReader): mutable.Buffer[(District, Polygon)] = {
    val districtList = districtsBroadcastVar.value
    districtList.asScala.flatMap { district =>
      val polygonStr = district.getPolygon
      if (!StringUtils.isEmpty(polygonStr)) {
        val wktPolygon = "POLYGON((" + polygonStr.replaceAll(",", " ").replaceAll(";", ",") + "))"
        val polygon = reader.read(wktPolygon).asInstanceOf[Polygon]
        Some((district, polygon))
      } else {
        None
      }
    }.toBuffer
  }

  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    new String(Base64.getEncoder.encode(proto.toByteArray))
  }
}