package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.config.Setup
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData, StageData}
import com.tmobile.sit.ignite.inflight.processing._
import org.apache.spark.sql.SparkSession

object Application extends Logger{

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

    implicit val runID = getRunId()
    implicit val loadDate = getLoadDate()

    logger.info(s"RunId: ${runID}")
    logger.info(s"LoadDate: ${loadDate}")

    logger.info("Getting SparkSession")
    implicit val sparkSession = getSparkSession()
    logger.info("Processing started - input data gathering")
    val inputFiles = new InputData(setup.settings.input)
    logger.info("Getting reference data from stage files")
    val refData = new ReferenceData(setup.settings.referenceData)

    logger.info("Preapring stage data")
    val stageData = new StageData(inputFiles)

    logger.info("Preparation for full-files processing")
    val fullOutput = new FullOutputsProcessor(stageData, setup.settings.appParams.filteredAirlineCodes.get)
    logger.info("Data for full-files")

    val fullOutputData = fullOutput.generateOutput()
    val fullOutputsWriter = new FullOutputWriter(setup.settings.output,fullOutputData)
    logger.info("Writing full files")
    fullOutputsWriter.writeOutput()
    logger.info("Full files DONE")

    logger.info("Preparing processor for RadiusCredit")
    val radiusCreditProcessor = new RadiusCreditDailyProcessor(refData,stageData, setup.settings.appParams.firstDate.get,
      setup.settings.appParams.firstPlus1Date.get, setup.settings.appParams.minRequestDate.get)

    logger.info("Creating RadiusCreditDaily data")
    val radiusCreditDailyData = radiusCreditProcessor.executeProcessing()

    logger.info("Preparing VoucherRadius processor")
    val voucherRadiusProcessor = new VoucherRadiusProcessor(stageData, refData, firstDate = setup.settings.appParams.firstDate.get,
      lastPlus1Date = setup.settings.appParams.firstPlus1Date.get, minRequestDate = setup.settings.appParams.minRequestDate.get)
    logger.info("Retrieving VoucherRadius data")
    val voucherRadiusData = voucherRadiusProcessor.getVchrRdsData()
    logger.info("Writing VoucherRadius data")
    val aggregatesWriter = new AggregatesWriter(radiusCreditDailyData, voucherRadiusData, outputConf = setup.settings.output)
    aggregatesWriter.writeOutput()
    logger.info("Processing DONE")
  }

}
