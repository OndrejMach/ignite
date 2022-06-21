package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.aggregateuau.AgregateUAUProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{AgregateUAUInputs, LookupsDataReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class calculates the UAU aggregates. Basically reads necessary inputs and triggers the calculation.
 * @param sparkSession
 * @param settings
 */
class AggregateUAU(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

  def processData(): DataFrame = {

    val activeUsers = new AgregateUAUInputs()

    val lookups = new LookupsDataReader()

    new AgregateUAUProcessor(activeUsers, lookups, settings.app.processingDate).result

  }

}
