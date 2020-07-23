package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.MAX_DATE
import com.tmobile.sit.ignite.rcse.structures.{CommonStructures, Terminal}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, upper}

class LookupsData(implicit sparkSession: SparkSession,settings: Settings) extends InputData(settings.app.processingDate) {
 import sparkSession.implicits._

  val client = {
    logger.info(s"Reading data from ${settings.stage.clientPath}")
    sparkSession.read.parquet(settings.stage.clientPath)
    /*
    CSVReader(
      path = settings.stage.clientPath,
      header = false,
      schema = Some(CommonStructures.clientSchema),
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      delimiter = "|"
    ).read()

     */
  }

  val tac = {
    logger.info(s"Reading data from ${settings.stage.tacPath}")
    sparkSession.read.parquet(settings.stage.tacPath)
/*
    CSVReader(
      path = settings.stage.tacPath,
      header = false,
      schema = Some(Terminal.tac_struct),
      delimiter = "|"
    ).read()

 */

      .filter($"valid_to" >= lit(MAX_DATE))
      .withColumn("terminal_id", $"id")
  }

  val terminal = {
    logger.info(s"Reading data from ${settings.stage.terminalPath}")
    sparkSession.read.parquet(settings.stage.terminalPath)
    /*
    CSVReader(path = settings.stage.terminalPath,
      header = false,
      schema = Some(Terminal.terminal_d_struct),
      delimiter = "|")
      .read()

     */
  }



  val terminalSW = {
    logger.info(s"Reading data from ${settings.stage.terminalSWPath}")
    sparkSession.read.parquet(settings.stage.terminalSWPath)
    /*
    CSVReader(path = settings.stage.terminalSWPath,
      header = false,
      schema = Some(CommonStructures.terminalSWSchema),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss")
      .read()

     */
      .filter($"modification_date".isNotNull)
      .withColumn("rcse_terminal_sw_desc", upper($"rcse_terminal_sw_desc"))
  }

}
