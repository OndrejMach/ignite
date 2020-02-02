package com.tmobile.sit.ignite.common.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

class ExcelWriter(filename: String, sheetName: Option[String], data: DataFrame) extends Writer {
  override def writeData(): Unit = {
    logger.info(s"Writing data to ${filename}, sheet: ${sheetName.getOrElse("default")}")
    data
      .coalesce(1)
      .write
      .format("com.crealytics.spark.excel")
      .option("sheetName", sheetName.getOrElse("Default"))
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode(SaveMode.Overwrite)
      .save(filename)
  }
}
