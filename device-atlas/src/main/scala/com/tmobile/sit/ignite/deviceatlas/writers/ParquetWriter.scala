package com.tmobile.sit.ignite.deviceatlas.writers

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, date_format}

abstract class ParquetWriter(implicit sparkSession: SparkSession) extends Logger {

  def writeParquet(data: DataFrame, path: String, partitioned: Boolean = false) = {
    data.cache()
    logger.info(s"Writing to path ${path}, rowcount: ${data.count()},  partitioned ${partitioned} ")
    val dataToWrite =
      (if (partitioned)
        data.withColumn("load_date", date_format(col("load_date"), "yyyy-MM-dd"))
      else
        data)
        //data
        .coalesce(1)
        .write

    val writer =
      if (partitioned) dataToWrite.partitionBy("load_date") else dataToWrite

    writer
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  def writeData(): Unit
}

