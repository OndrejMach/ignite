package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.SparkSession

/**
 * Inputs for the UAU aggregates processing
 * @param sparkSession
 * @param settings - paths to the inputs
 */

class AgregateUAUInputs(implicit sparkSession: SparkSession, settings: Settings) extends InputData(settings.app.processingDate) {
  val activeUsersData = {
    logger.info(s"Reading data from ${settings.stage.activeUsers}${todaysPartition}")

    sparkSession.read.parquet(s"${settings.stage.activeUsers}${todaysPartition}")

  }
}
