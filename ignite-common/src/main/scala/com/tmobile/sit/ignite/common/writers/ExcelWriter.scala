package com.tmobile.sit.ignite.common.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

class ExcelWriter(filename: String,  data: DataFrame, sheetName: String = "", cellRange: String = "A1" ) extends Writer {
  override def writeData(): Unit = {
    logger.info(s"Writing data to ${filename}, sheet: ${sheetName}")
    data
      .coalesce(1)
      .write
      .format("com.crealytics.spark.excel")
      .option("sheetName", s"${sheetName}${cellRange}")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode(SaveMode.Overwrite)
      .save(filename)
  }
}
