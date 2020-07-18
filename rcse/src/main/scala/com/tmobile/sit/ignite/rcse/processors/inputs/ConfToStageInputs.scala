package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.SparkSession

/**
 * Inputs fot the Conf calculation
 * @param sparkSession
 * @param settings- input paths
 */

class ConfToStageInputs(implicit sparkSession: SparkSession,settings: Settings) extends InputData(settings.app.processingDate) {
  val events = {
    logger.info(s"Reading data from ${settings.stage.dmEventsFile}${todaysPartition}")
    sparkSession.read.parquet(settings.stage.dmEventsFile + todaysPartition)
  }

  val confData = {
    logger.info(s"Reading data from ${settings.stage.confFile}${yesterdaysPartition}")
    sparkSession.read.parquet(s"${settings.stage.confFile}${yesterdaysPartition}")
  }
}
