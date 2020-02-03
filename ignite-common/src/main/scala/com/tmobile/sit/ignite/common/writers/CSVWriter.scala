package com.tmobile.sit.ignite.common.writers

import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVWriter(path: String, data: DataFrame, delimiter: String = ",", writeHeader: Boolean = true)(implicit sparkSession: SparkSession) extends Writer {
  //EOL, Encoding???, encoding,
  def writeData() : Unit = {
    logger.info(s"Writing data to ${path} " )
    data
      .coalesce(1)
      .write
      .option("header", if (writeHeader) "true" else "false")
      .csv(path)
  }
}
