package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.SparkSession

/**
 * Inputs for the Init user aggregates
 * @param sparkSession
 * @param settings - input paths
 */

class InitUserInputs(implicit sparkSession: SparkSession,settings: Settings) extends InputData(settings.app.processingDate) {
  val confData = {
    logger.info(s"Reading data from ${settings.stage.confFile}${todaysPartition}")
    sparkSession.read.parquet(s"${settings.stage.confFile}${todaysPartition}")

  }

  val initData = {
    logger.info(s"Reading data from ${settings.stage.initUser + yesterdaysPartition}")
    sparkSession.read.parquet(settings.stage.initUser + yesterdaysPartition )
  }
}
