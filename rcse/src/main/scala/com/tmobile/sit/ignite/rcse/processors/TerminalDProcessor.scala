package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsDataReader
import com.tmobile.sit.ignite.rcse.processors.terminald.UpdateDTerminal
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Processor class for termianl_d calculation - initialised inputs and processes data
 * @param sparkSession
 * @param settings - paths to the input files are read
 */

class TerminalDProcessor(implicit sparkSession: SparkSession,settings: Settings ) extends Logger {
   def processData(): DataFrame = {
    val lookups = new LookupsDataReader()

    new UpdateDTerminal(lookups.terminal, lookups.tac, settings.app.maxDate).getData()

  }

}
