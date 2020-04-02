package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

trait OutputWriter {
  def writeOutput() : Unit
}

abstract class InflightWriter(timestampFormat: String) extends OutputWriter with Logger {

  def writeOut(path: String, data: DataFrame)(implicit sparkSession: SparkSession) = {
    logger.info(s"Writing output file ${path}")
    val writer = CSVWriter(data, path, delimiter = "|", timestampFormat = timestampFormat)
    writer.writeData()
  }
}