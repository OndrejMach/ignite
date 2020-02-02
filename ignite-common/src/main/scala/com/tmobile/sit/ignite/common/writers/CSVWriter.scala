package com.tmobile.sit.ignite.common.writers

import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVWriter(path: String, data: DataFrame)(implicit sparkSession: SparkSession) extends Writer {
  def writeData() : Unit = {
    logger.info(s"Writing data to ${path} " )
    data
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(path)
  }
}
