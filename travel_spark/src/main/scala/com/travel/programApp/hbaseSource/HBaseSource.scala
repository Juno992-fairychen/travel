package com.travel.programApp.hbaseSource

import java.util
import java.io.Serializable
import com.travel.common.Constants
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan => HBaseScan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

// 1. 数据源注册
class HBaseSource extends DataSourceRegister with TableProvider {
  override def shortName(): String = "hbase-read"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val schemaStr = options.get("sparksql_table_schema")
    if (schemaStr != null && schemaStr.nonEmpty) StructType.fromDDL(schemaStr)
    else StructType(Seq(StructField("id", StringType)))
  }

  override def getTable(schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): Table = {
    new HBaseSourceTable(schema, properties)
  }
}

// 2. 表描述类 - 增加了 override 关键字
class HBaseSourceTable(
                        val structType: StructType,
                        override val properties: util.Map[String, String] // 关键修复：增加 override
                      ) extends Table with SupportsRead with Serializable {

  private val tableName = properties.get("hbase_table_name")
  private val hbaseTableSchema = properties.get("hbase_table_schema")

  // Driver端校验
  if (tableName == null) {
    throw new IllegalArgumentException(s"HBaseSource 找不到 hbase_table_name！可用 keys: ${properties.keySet()}")
  }

  override def name(): String = tableName
  override def schema(): StructType = structType
  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new HBaseScanBuilder(tableName, hbaseTableSchema, structType)
  }
}

// 3. ScanBuilder
class HBaseScanBuilder(val tableName: String, val hbaseTableSchema: String, val schema: StructType)
  extends ScanBuilder with Serializable {
  override def build(): Scan = new HBaseScanImpl(tableName, hbaseTableSchema, schema)
}

// 4. Scan实现
class HBaseScanImpl(val tableName: String, val hbaseTableSchema: String, val schema: StructType)
  extends Scan with Batch with Serializable {
  override def readSchema(): StructType = schema
  override def toBatch: Batch = this
  override def planInputPartitions(): Array[InputPartition] = {
    //Array(new HBaseInputPartition())只有一个分区
    val conn = HbaseTools.getHbaseConn  // ← 复用单例连接
    val locator = conn.getRegionLocator(TableName.valueOf(tableName))
    try {
      val startKeys = locator.getStartKeys
      startKeys.zipWithIndex.map { case (startKey, i) =>
        val endKey = if (i + 1 < startKeys.length) startKeys(i + 1) else Array.empty[Byte]
        new HBaseInputPartition(i, startKey, endKey)
      }.toArray
    } finally {
      locator.close()  // 只关 locator，不关 connection（单例还要复用）
    }
  }
  override def createReaderFactory(): PartitionReaderFactory = new HBasePartitionReaderFactory(tableName, hbaseTableSchema)
}

// 5. 分片
class HBaseInputPartition(val index: Int, val startKey: Array[Byte], val endKey: Array[Byte]) extends InputPartition with Serializable

// 6. 工厂
class HBasePartitionReaderFactory(val tableName: String, val hbaseTableSchema: String)
  extends PartitionReaderFactory with Serializable {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    //new HBasePartitionReader(tableName, hbaseTableSchema)
    val p = partition.asInstanceOf[HBaseInputPartition]
    new HBasePartitionReader(tableName, hbaseTableSchema, p.startKey, p.endKey)
  }
}

// 7. 读取器
class HBasePartitionReader(val tableName: String, val hbaseTableSchema: String, val startKey: Array[Byte], val endKey: Array[Byte])
  extends PartitionReader[InternalRow] {

  private lazy val connection = HbaseTools.getHbaseConn
  private lazy val table = connection.getTable(TableName.valueOf(tableName))

  // 关键：根据分区范围设置 Scan
  private lazy val scan = {
    val s = new HBaseScan()
    if (startKey.nonEmpty) s.withStartRow(startKey)
    if (endKey.nonEmpty) s.withStopRow(endKey)
    s
  }

  private lazy val scanner = table.getScanner(scan)
  private lazy val iterator = scanner.iterator()

  private val columns = hbaseTableSchema.split(",").map(_.trim).map { col =>
    if (col.contains(":")) {
      val parts = col.split(":")
      (parts(0), parts(1))
    } else (Constants.DEFAULT_DB_FAMILY, col)
  }

  override def next(): Boolean = iterator.hasNext
  override def get(): InternalRow = {
    val result = iterator.next()
    val values = columns.map { case (cf, col) =>
      val valueBytes = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(col))
      UTF8String.fromString(if (valueBytes == null) "" else Bytes.toString(valueBytes))
    }
    InternalRow.fromSeq(values)
  }
  override def close(): Unit = {
    if (scanner != null) scanner.close()
    if (table != null) table.close()
  }
}
