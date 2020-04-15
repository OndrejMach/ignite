package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.ignite.inflight.config.Settings
import com.tmobile.sit.ignite.inflight.processing._
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData}
import com.tmobile.sit.ignite.inflight.processing.writers.DailyWriter
import org.apache.spark.sql.SparkSession

/**
 * Class reporesenting daily calculation
 * @param inputData - input data
 * @param refData - reference data - orderDB, map voucher, exchange rates
 * @param dailyWriter - writer generating final output files
 * @param sparkSession - spark
 * @param settings - application parameters from the configuration file
 */
class DailyCalculation(inputData: InputData, refData: ReferenceData, dailyWriter: DailyWriter)(implicit sparkSession: SparkSession,settings: Settings) extends Processor{

  override def executeCalculation() {
    val dailyProcessor = new DailyProcessorImpl(settings = settings, inputFiles = inputData, refData = refData)

    val inflightOutputs = dailyProcessor.runDailyProcessing()

    dailyWriter.writeDailyData(inflightOutputs)

    logger.info("Processing DONE")
  }

}
