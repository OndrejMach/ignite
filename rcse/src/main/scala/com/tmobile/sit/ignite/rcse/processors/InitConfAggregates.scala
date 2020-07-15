package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.initconfaggregates.InitConfAggregatesProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{InitConfInputs, LookupsData}
import org.apache.spark.sql.{SaveMode, SparkSession}

class InitConfAggregates(processingDate: Date, settings: Settings)(implicit sparkSession: SparkSession) extends Processor {

  override def processData(): Unit = {
    logger.info("Getting input data")
    val inputData: InitConfInputs = new InitConfInputs(settings = settings)
    logger.info("Getting lookups data")
    val lookups = new LookupsData(settings)
    logger.info("Processing initConf Aggregates")
    val result = new InitConfAggregatesProcessor(lookups = lookups, inputData = inputData, maxDate = MAX_DATE, processingDate = processingDate).getData

    logger.info(s"result count ${result.count()}")

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
  }
}
