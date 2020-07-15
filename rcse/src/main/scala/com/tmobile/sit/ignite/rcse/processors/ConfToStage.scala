package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.conf.ConfProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{ConfToStageInputs, LookupsData}
import org.apache.spark.sql.SparkSession

class ConfToStage(settings: Settings, max_Date: Date, processing_date: Date)(implicit sparkSession: SparkSession) extends Processor {

  override def processData(): Unit = {
    val lookups = new LookupsData(settings)
    val inputs = new ConfToStageInputs((settings))

    val processor = new ConfProcessor(lookups = lookups, inputs = inputs, max_Date = max_Date, processing_date = processing_date)

    processor.result
      .coalesce(1)
      .write
      .option("delimiter", "|")
      .option("header", "false")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_f_rcse_conf.TMD.csv");

    logger.info(s"Conf file row count: ${result.count()}")
  }


}
