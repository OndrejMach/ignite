package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{Conf, InitUsers}
import org.apache.spark.sql.SparkSession

class InitUserInputs(settings: Settings)(implicit sparkSession: SparkSession) extends Logger{
  val confData = CSVReader(
    path = settings.confFile,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
    schema = Some(Conf.confFileSchema),
    header = false,
    delimiter = "|"
  )
    .read()

  val initData = CSVReader(
    path = settings.initUser,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_x_rcse_init_user.TMD.csv",
    header = false,
    delimiter = "|",
    schema = Some(InitUsers.initUsersSchema)
  )
    .read()
}
