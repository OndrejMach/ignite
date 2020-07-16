package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{Conf, Terminal}
import org.apache.spark.sql.SparkSession

class ConfToStageInputs(implicit sparkSession: SparkSession,settings: Settings) extends Logger{
  val events = {
    logger.info(s"Reading data from ${settings.stage.terminalPath}")
    CSVReader(path = settings.stage.terminalPath,
      header = false,
      schema = Some(Terminal.terminalSchema),
      delimiter = "|")
      .read()
  }

  val confData = {
    logger.info(s"Reading data from ${settings.stage.confFile}")
    CSVReader(
      path = settings.stage.confFile,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      header = false,
      schema = Some(Conf.confFileSchema),
      delimiter = "|"
    ).read()
  }
}
