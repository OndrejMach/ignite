package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{Conf, InitConf}
import org.apache.spark.sql.SparkSession

class InitConfInputs(settings: Settings)(implicit sparkSession: SparkSession) extends Logger {
  val confData = CSVReader(
    path = settings.confFile,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
    schema = Some(Conf.confFileSchema),
    header = false,
    delimiter = "|"
  ).read()

  val initData = CSVReader(
    path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_x_rcse_init_conf.TMD.csv",
    header = false,
    delimiter = "|",
    schema = Some(InitConf.initConfSchema)
  )
    .read()

}
