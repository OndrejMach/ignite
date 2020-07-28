package com.tmobile.sit.ignite.deviceatlas.writers

import java.sql.Date
import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

abstract class ParquetWriter (processingDate: Date)
                    (implicit sparkSession: SparkSession) extends Logger {


  def writeParquet(data: DataFrame, path: String, partitioned: Boolean = false) = {
    data.cache()
    logger.info(s"Writing to path ${path}, rowcount: ${data.count()}")
    val dataToWrite =
      (if (partitioned) data.withColumn("load_date", lit(processingDate)) else data)
        .coalesce(1)
        .write

    val writer =
      if (partitioned) dataToWrite.partitionBy("date") else dataToWrite

    writer
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  def writeData(): Unit
}

