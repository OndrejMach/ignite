package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.initconfaggregates.InitConfAggregatesProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{InitConfInputs, LookupsDataReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * this class generates data for the init conf aggregates - reads inputs and generates resulting data.
 * @param sparkSession
 * @param settings -paths for inputs
 */

class InitConfAggregates(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

  def processData(): DataFrame = {
    logger.info("Getting input data")
    val inputData: InitConfInputs = new InitConfInputs()
    logger.info("Getting lookups data")
    val lookups = new LookupsDataReader()
    logger.info("Processing initConf Aggregates")
    new InitConfAggregatesProcessor(lookups = lookups, inputData = inputData, maxDate = settings.app.maxDate, processingDate = settings.app.processingDate).getData

  }
}
