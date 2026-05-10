package com.travel.programApp.hbaseSink

import java.util
import java.io.Serializable
import com.travel.common.Constants
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class HBaseSink extends DataSourceRegister with TableProvider {
  override def shortName(): String = "hbase-write"
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType.fromDDL(options.get("sparksql_table_schema"))
  }
  override def getTable(schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): Table = {
    new HBaseSinkTable(schema, properties)
  }
}

class HBaseSinkTable(
                      val structType: StructType,
                      override val properties: util.Map[String, String] // 关键修复：增加 override
                    ) extends Table with SupportsWrite with Serializable {

  private val tableName = properties.get("hbase_table_name")
  private val hbaseTableSchema = properties.get("hbase_table_schema")

  if (tableName == null) {
    throw new IllegalArgumentException(s"HBaseSink 找不到 hbase_table_name！可用 keys: ${properties.keySet()}")
  }

  override def name(): String = tableName
  override def schema(): StructType = structType
  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(TableCapability.BATCH_WRITE)
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new HBaseWriteBuilder(tableName, hbaseTableSchema)
}

class HBaseWriteBuilder(val tableName: String, val hbaseTableSchema: String)
  extends WriteBuilder with Serializable {
  override def buildForBatch(): BatchWrite = {
    val families = hbaseTableSchema.split(",").map(_.trim)
      .map(s => if(s.contains(":")) s.split(":")(0) else Constants.DEFAULT_DB_FAMILY).distinct

    val conn = HbaseTools.getHbaseConn
    val admin = conn.getAdmin
    try {
      val tn = TableName.valueOf(tableName)
      if (!admin.tableExists(tn)) {
        HbaseTools.createTablesIfNotExist(Array(tableName), families)
      }
    } finally { admin.close() }

    new HBaseBatchWrite(tableName, hbaseTableSchema)
  }
}

class HBaseBatchWrite(val tableName: String, val hbaseTableSchema: String)
  extends BatchWrite with Serializable {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new HBaseDataWriterFactory(tableName, hbaseTableSchema)
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class HBaseDataWriterFactory(val tableName: String, val hbaseTableSchema: String)
  extends DataWriterFactory with Serializable {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[org.apache.spark.sql.catalyst.InternalRow] =
    new HBaseDataWriter(tableName, hbaseTableSchema)
}

class HBaseDataWriter(val tableName: String, val hbaseTableSchema: String)
  extends DataWriter[org.apache.spark.sql.catalyst.InternalRow] {
  private val connection = HbaseTools.getHbaseConn
  private val table = connection.getTable(TableName.valueOf(tableName))
  private val colMappings = hbaseTableSchema.split(",").map(_.trim).drop(1).map { col =>
    if (col.contains(":")) { val pts = col.split(":"); (pts(0), pts(1)) }
    else (Constants.DEFAULT_DB_FAMILY, col)
  }

  override def write(record: org.apache.spark.sql.catalyst.InternalRow): Unit = {
    val rowkey = record.getUTF8String(0)
    if (rowkey == null) return
    val put = new Put(rowkey.getBytes)
    for (i <- colMappings.indices) {
      val value = record.getUTF8String(i + 1)
      if (value != null) {
        val (cf, q) = colMappings(i)
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(q), value.getBytes)
      }
    }
    table.put(put)
  }
  override def commit(): WriterCommitMessage = { close(); null }
  override def abort(): Unit = close()
  override def close(): Unit = { if (table != null) table.close() }
}
