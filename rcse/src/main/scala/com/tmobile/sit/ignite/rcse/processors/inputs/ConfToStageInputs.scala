package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Inputs fot the Conf calculation
 * @param sparkSession
 * @param settings- input paths
 */

class ConfToStageInputs(implicit sparkSession: SparkSession,settings: Settings) extends InputData(settings.app.processingDate) {
  val events = {
    val ret = sparkSession.read.parquet(s"${settings.stage.dmEventsFile}${todaysPartition}").repartition(10)
    logger.info(s"Reading data from ${settings.stage.dmEventsFile}${todaysPartition}, count: ${ret.count()}")
    ret
  }

  val confData = {
    val ret = sparkSession.read.parquet(s"${settings.stage.confFile}${yesterdaysPartition}").repartition(10)
    logger.info(s"Reading data from ${settings.stage.confFile}${yesterdaysPartition}, count: ${ret.count()}")
    ret
  }
}
