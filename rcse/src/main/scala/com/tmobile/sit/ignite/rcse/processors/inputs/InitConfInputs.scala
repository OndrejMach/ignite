package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{Conf, InitConf}
import org.apache.spark.sql.SparkSession

class InitConfInputs(implicit sparkSession: SparkSession,settings: Settings) extends Logger {
  val confData = {
    logger.info(s"Reading data from ${settings.stage.confFile}")
    CSVReader(
      path = settings.stage.confFile,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      schema = Some(Conf.confFileSchema),
      header = false,
      delimiter = "|"
    ).read()
  }

  val initData = {
    logger.info(s"Reading data from ${settings.stage.initConf}")
    CSVReader(
      path = settings.stage.initConf,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_x_rcse_init_conf.TMD.csv",
      header = false,
      delimiter = "|",
      schema = Some(InitConf.initConfSchema)
    )
      .read()
  }

}
