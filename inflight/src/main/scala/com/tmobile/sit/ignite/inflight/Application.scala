package com.tmobile.sit.ignite.inflight

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.inflight.config.Setup
import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregateRadiusCredit, AggregateRadiusCreditData}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, OutputFilters, ReferenceData, StageData}
import com.tmobile.sit.ignite.inflight.processing.{FullOutputsProcessor, RadiusVoucheProcessor}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application extends Logger{

  private def processAggregateRadiusCredit()(implicit sparkSession: SparkSession): Unit = {

  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Reading configuation files")
    val setup = new Setup()
    logger.info(s"Configuration parameters check")
    if (!setup.settings.isAllDefined){
      logger.error("Application parameters not properly defined")
      setup.settings.printMissingFields()
    }
    logger.info("Configuration parameters OK")
    setup.settings.printAllFields()
    val runID = getRunId()
    val loadDate = getLoadDate()

    logger.info(s"RunId: ${runID}")
    logger.info(s"LoadDate: ${loadDate}")

    logger.info("Getting SparkSession")
    implicit val sparkSession = getSparkSession()
    logger.info("Processing started - input data gathering")
    val inputFiles = new InputData(setup.settings.input)
    logger.info("Getting reference data from stage files")
    val refData = new ReferenceData(setup.settings.referenceData)

    logger.info("Preapring stage processor")
    val stage = new StageData(inputFiles, runId = runID, loadDate = loadDate)
    logger.info("Preparation for full-files processing")
    val fullOutput = new FullOutputsProcessor(stage, setup.settings.output, setup.settings.appParams.filteredAirlineCodes.get)
    logger.info("Data for full-files output ready")

    logger.info("Preparing processor for RadiusVoucher")
    val radiusVoucheProcessor = new RadiusVoucheProcessor(refData,stage, setup.settings.appParams.firstDate.get,
      setup.settings.appParams.firstPlus1Date.get, setup.settings.appParams.minRequestDate.get,
      runID, loadDate)

    val radiusVoucherData = radiusVoucheProcessor.executeProcessing()
    radiusVoucherData.show(false)








    fullOutput.writeOutput()





  }

}
