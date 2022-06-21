package com.tmobile.sit.ignite.common.common.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Excel writer class writes standard MS format. Implements trait Writer. You can specify a particula sheet and cell range for writing.
 * @author Ondrej Machacek
 * @param filename - resulting excel filename
 * @param sheetName - sheet name to write into
 * @param cellRange - cell range for writing within the sheet specified
 */
class ExcelWriter(data: DataFrame,filename: String,  sheetName: String = "", cellRange: String = "A1" ) extends Writer {
  override def writeData(): Unit = {
    logger.info(s"Writing data to ${filename}, sheet: ${sheetName}")
    data
      .repartition(1)
      .write
      .format("com.crealytics.spark.excel")
      .option("sheetName", s"${sheetName}${cellRange}")
      .option("header", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode(SaveMode.Overwrite)
      .save(filename)



  }
}

object ExcelWriter {
  def apply(data: DataFrame,
             filename: String,
            sheetName: String = "",
            cellRange: String = "A1" ): ExcelWriter = new ExcelWriter(data: DataFrame,filename, sheetName, cellRange)
}
