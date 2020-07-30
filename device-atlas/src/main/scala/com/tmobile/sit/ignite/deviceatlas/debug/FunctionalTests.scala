package com.tmobile.sit.ignite.deviceatlas.debug

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.common.readers.TacStageReader
import com.tmobile.sit.ignite.deviceatlas.config.Setup
import com.tmobile.sit.ignite.deviceatlas.data.FileStructures
import com.tmobile.sit.ignite.deviceatlas.getSparkSession
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

  logger.info(s"Reading csv file")
  val cptm_ta_d_tac = CSVReader("D:\\spark\\workspace\\deviceatlas\\input\\cptm_ta_d_tac.csv",
    header = false,
    schema = Some(FileStructures.cptm_ta_d_tac),
    delimiter = "|"
  ).read()

  logger.info(s"Writing parquet files to stage")
  StageWriter(stageData = cptm_ta_d_tac, partitioned = true,
    path = s"D:\\spark\\workspace\\deviceatlas\\stage\\cptm_ta_d_tac.parquet").writeData()

  val cpt_ta_d_tac_parquet = {
    logger.info("Reading final Exchange rates")
    TacStageReader(s"D:\\spark\\workspace\\deviceatlas\\stage\\cptm_ta_d_tac.parquet").read()
  }

  cpt_ta_d_tac_parquet.show(5)

}
