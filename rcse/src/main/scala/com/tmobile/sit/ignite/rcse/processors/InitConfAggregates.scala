package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.initconfaggregates.InitConfAggregatesProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{InitConfInputs, LookupsData}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class InitConfAggregates(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

  def processData(): DataFrame = {
    logger.info("Getting input data")
    val inputData: InitConfInputs = new InitConfInputs()
    logger.info("Getting lookups data")
    val lookups = new LookupsData()
    logger.info("Processing initConf Aggregates")
    new InitConfAggregatesProcessor(lookups = lookups, inputData = inputData, maxDate = settings.app.maxDate, processingDate = settings.app.processingDate).getData


    /*
    result
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "false")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_x_rcse_init_conf.TMD.csv");

     */
  }
}
