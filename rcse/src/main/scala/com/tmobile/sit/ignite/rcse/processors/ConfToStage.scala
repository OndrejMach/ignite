package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.conf.ConfProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{ConfToStageInputs, LookupsData, LookupsDataReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * this one generates the conf stage file. Reads inputs and triggers data calculation
 * @param sparkSession
 * @param settings - paths for the input data
 */

class ConfToStage(implicit sparkSession: SparkSession,settings: Settings) extends Logger {

  def processData(): DataFrame = {
    val lookups = new LookupsDataReader()
    val inputs = new ConfToStageInputs()

    new ConfProcessor(lookups = lookups, inputs = inputs, max_Date = settings.app.maxDate, processing_date = settings.app.processingDate).result

  }


}
