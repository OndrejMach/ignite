package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.conf.ConfProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{ConfToStageInputs, LookupsData}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class ConfToStage(implicit sparkSession: SparkSession,settings: Settings) extends Logger {

  def processData(): DataFrame = {
    val lookups = new LookupsData()
    val inputs = new ConfToStageInputs()

    new ConfProcessor(lookups = lookups, inputs = inputs, max_Date = settings.app.maxDate, processing_date = settings.app.processingDate).result
/*
    processor.result
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "false")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_f_rcse_conf.TMD.csv");


 */
  }


}
