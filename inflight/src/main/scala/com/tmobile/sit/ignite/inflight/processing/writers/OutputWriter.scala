package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

trait OutputWriter extends Logger {
  def writeOutput() : Unit
}


abstract class InflightWriterUTF8Char(timestampFormat: String) extends OutputWriter  {
  private val UTF8CHAR = "\ufeff"

  def writeData(path: String, data: DataFrame)(implicit sparkSession: SparkSession) = {
    logger.info(s"Writing output file ${path}")
    val firstColumn = data.columns(0)
    val writer = CSVWriter(data.withColumnRenamed(firstColumn,UTF8CHAR+firstColumn), path, delimiter = "|", timestampFormat = timestampFormat)
    writer.writeData()
  }
}