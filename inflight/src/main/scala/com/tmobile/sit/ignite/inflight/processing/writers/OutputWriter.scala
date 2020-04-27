package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

trait OutputWriter extends Logger {
  def writeOutput() : Unit
}

/**
 * A helper class for writing CSV file - this one puts the BOM character at the very beginning to make cleat that this is the UTF-8 encoded file
 * @param timestampFormat - format how Timestamps should be represented as strings
 */
abstract class InflightWriterUTF8Char(timestampFormat: String) extends OutputWriter  {
  private val UTF8CHAR = "\ufeff"

  def writeData(path: String, data: DataFrame, writeHeader: Boolean = true)(implicit sparkSession: SparkSession) = {
    logger.info(s"Writing output file ${path}")
    val firstColumn = data.columns(0)
    val writer = CSVWriter(data.withColumnRenamed(firstColumn,UTF8CHAR+firstColumn), path, delimiter = "|", timestampFormat = timestampFormat, writeHeader = writeHeader)
    writer.writeData()
  }
}