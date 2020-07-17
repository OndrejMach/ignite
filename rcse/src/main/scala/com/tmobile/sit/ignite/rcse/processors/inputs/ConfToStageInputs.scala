package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.SparkSession

class ConfToStageInputs(implicit sparkSession: SparkSession,settings: Settings) extends InputData(settings.app.processingDate) {
  val events = {
    logger.info(s"Reading data from ${settings.stage.dmEventsFile}${todaysPartition}")
    sparkSession.read.parquet(settings.stage.dmEventsFile + todaysPartition)

    /*CSVReader(path = settings.stage.terminalPath,
      header = false,
      schema = Some(Terminal.terminalSchema),
      delimiter = "|")
      .read()

     */
  }

  val confData = {
    logger.info(s"Reading data from ${settings.stage.confFile}${yesterdaysPartition}")
    sparkSession.read.parquet(s"${settings.stage.confFile}${yesterdaysPartition}")
    /*
    CSVReader(
      path = settings.stage.confFile,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      header = false,
      schema = Some(Conf.confFileSchema),
      delimiter = "|"
    ).read()

     */
  }
}
