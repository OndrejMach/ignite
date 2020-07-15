package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{Conf, Terminal}
import org.apache.spark.sql.SparkSession

class ConfToStageInputs(settings: Settings)(implicit sparkSession: SparkSession) extends Logger{
  val events = CSVReader(path = settings.terminalPath,
    header = false,
    schema = Some(Terminal.terminalSchema),
    delimiter = "|")
    .read()

  val confData = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
    header = false,
    schema = Some(Conf.confFileSchema),
    delimiter = "|"
  ).read()
}
