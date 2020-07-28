package com.tmobile.sit.ignite.deviceatlas

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.{CSVReader, Reader}
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.deviceatlas.config.Setup
import com.tmobile.sit.ignite.deviceatlas.datastructures.FileStructures
import com.tmobile.sit.ignite.deviceatlas.writers.StageWriter

object ParquetTest extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()

  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  println("Web UI: " + sparkSession.sparkContext.uiWebUrl)

  val cptm_ta_d_tac = CSVReader("D:\\spark\\workspace\\deviceatlas\\input\\cptm_ta_d_tac.csv",
    header = false,
    schema = Some(FileStructures.cptm_ta_d_tac),
    delimiter = "|"
  ).read()

  val dateNow = new java.sql.Date(System.currentTimeMillis())
  logger.info(s"Writing parquet files to stage")
  new StageWriter(processingDate = dateNow, stageData = cptm_ta_d_tac,
    path = s"D:\\spark\\workspace\\deviceatlas\\output\\cptm_ta_d_tac.parquet", partitioned = true).writeData()

}
