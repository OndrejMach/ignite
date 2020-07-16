package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{ActiveUsers, Conf, RegDer}
import org.apache.spark.sql.SparkSession

class ActiveUsersInputs(implicit sparkSession: SparkSession,settings: Settings) extends Logger {
  val inputEvents = {
    logger.info(s"Reading data from ${settings.stage.regDerEventsToday}")
    CSVReader(
      path = settings.stage.regDerEventsToday,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_events.TMD.20200607.reg_der.csv",
      delimiter = "|",
      header = false,
      schema = Some(RegDer.regDerSchema)
    ).read()
  }

  val inputConf = {
    logger.info(s"Reading data from ${settings.stage.confFile}")
    CSVReader(
      path = settings.stage.confFile,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      delimiter = "|",
      header = false,
      schema = Some(Conf.confFileSchema)
    )
      .read()
      .select("msisdn", "rcse_tc_status_id", "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id")
  }

  val activeUsersYesterday = {
    logger.info(s"Reading data from ${settings.stage.activeUsersYesterday}")
    CSVReader(
      path = settings.stage.activeUsersYesterday,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_active_user.TMD.20200606.csv.gz",
      delimiter = "|",
      header = false,
      schema = Some(ActiveUsers.activeUsersSchema)
    ).read()
  }

  val eventsYesterday = {
    logger.info(s"Reading data from ${settings.stage.regDerEventsYesterday}")
    CSVReader(
      path = settings.stage.regDerEventsYesterday,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_events.TMD.20200606.reg_der.csv.gz",
      delimiter = "|",
      header = false,
      schema = Some(RegDer.regDerSchema)
    ).read()
  }
}
