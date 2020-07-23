package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.SparkSession

/**
 * Input data for the Active user calculation
 * @param sparkSession
 * @param settings - paths for the inputs
 */

class ActiveUsersInputs(implicit sparkSession: SparkSession, settings: Settings) extends InputData(settings.app.processingDate) {

  val inputEvents = {
    logger.info(s"Reading data from ${settings.stage.regDerEvents}${todaysPartition}")
    sparkSession.read.parquet(s"${settings.stage.regDerEvents}${todaysPartition}")
  }

  val inputConf = {
    logger.info(s"Reading data from ${settings.stage.confFile}${todaysPartition}")
    sparkSession.read.parquet(s"${settings.stage.confFile}${todaysPartition}")
      .select("msisdn", "rcse_tc_status_id", "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id")
  }

  val activeUsersYesterday = {
    logger.info(s"Reading data from ${settings.stage.activeUsers}${yesterdaysPartition}")
    sparkSession.read.parquet(s"${settings.stage.activeUsers}${yesterdaysPartition}")
  }

  val eventsYesterday = {
    logger.info(s"Reading data from ${settings.stage.regDerEvents}${yesterdaysPartition}")

    sparkSession.read.parquet(s"${settings.stage.regDerEvents}${yesterdaysPartition}")
  }
}
