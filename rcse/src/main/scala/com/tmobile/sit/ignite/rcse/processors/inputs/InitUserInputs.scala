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
    val conf = sparkSession.read.parquet(s"${settings.stage.confFile}${todaysPartition}")
    conf.show(false)
    conf

  }

  val initData = {
    logger.info(s"Reading data from ${settings.stage.initUser + yesterdaysPartition}")
    val data = sparkSession.read.parquet(settings.stage.initUser + yesterdaysPartition )
    logger.info(s"InitConf count: ${data.count()}")
    data.show(false)
    data
  }
}
