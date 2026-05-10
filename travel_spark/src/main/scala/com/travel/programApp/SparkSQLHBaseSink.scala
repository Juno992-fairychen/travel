package com.travel.programApp

import org.apache.spark.sql.{DataFrame, SaveMode}

object SparkSQLHBaseSink {
  def saveToHBase(dataFrame: DataFrame, tableName: String, hbaseTableSchema: String): Unit = {
    dataFrame.write
      .format("hbase-write")
      .mode(SaveMode.Append) // 改为 Append，避开 Spark 的 Truncate 检查
      .option("hbase_table_name", tableName)
      .option("hbase_table_schema", hbaseTableSchema)
      .option("sparksql_table_schema", dataFrame.schema.toDDL)
      .save()
  }
}
