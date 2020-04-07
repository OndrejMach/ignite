package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.ignite.inflight.config.Settings
import com.tmobile.sit.ignite.inflight.processing._
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData}
import com.tmobile.sit.ignite.inflight.processing.writers.DailyWriter
import org.apache.spark.sql.SparkSession

class DailyCalculation(inputData: InputData, refData: ReferenceData, dailyWriter: DailyWriter)(implicit sparkSession: SparkSession,settings: Settings) extends Processor{

  override def executeCalculation() {
    //implicit val runID = getRunId()
    //implicit val loadDate = getLoadDate()

    //logger.info(s"RunId: ${runID}")
    //.info(s"LoadDate: ${loadDate}")
  /*
    logger.info("Processing started - input data gathering")
    val inputFiles = new InputData(settings.input)
    logger.info("Getting reference data from stage files")
    val refData = new ReferenceData(settings.referenceData)
*/
    val dailyProcessor = new DailyProcessorImpl(settings = settings, inputFiles = inputData, refData = refData)

    val inflightOutputs = dailyProcessor.runDailyProcessing()

    dailyWriter.writeDailyData(inflightOutputs)

    /*
    logger.info("Preapring stage data")
    val stageData = new StageData(inputFiles)

    logger.info("Preparation for full-files processing")
    val fullOutput = new FullOutputsProcessor(stageData, settings.appParams.filteredAirlineCodes.get)
    logger.info("Data for full-files")

    val fullOutputData = fullOutput.generateOutput()



    val fullOutputsWriter = new FullOutputWriter(settings.output,fullOutputData)
    logger.info("Writing full files")
    fullOutputsWriter.writeOutput()
    logger.info("Full files DONE"

    logger.info("Preparing staged reference data (Exchange Rates)")
    val refDataStaged = new NormalisedExchangeRates(exchangeRates = refData.exchangeRates, settings.appParams.minRequestDate.get)

    logger.info("Preparing processor for RadiusCredit")
    val radiusCreditProcessor = new RadiusCreditDailyProcessor(refData,stageData, refDataStaged,settings.appParams.firstDate.get,
      settings.appParams.firstPlus1Date.get, settings.appParams.minRequestDate.get)

    logger.info("Creating RadiusCreditDaily data")
    val radiusCreditDailyData = radiusCreditProcessor.executeProcessing()

    logger.info("Preparing VoucherRadius processor")
    val voucherRadiusProcessor = new VoucherRadiusProcessor(stageData, refData, refDataStaged,firstDate = settings.appParams.firstDate.get,
      lastPlus1Date = settings.appParams.firstPlus1Date.get, minRequestDate = settings.appParams.minRequestDate.get)
    logger.info("Retrieving VoucherRadius data")
    val voucherRadiusData = voucherRadiusProcessor.getVchrRdsData()

    logger.info("Writing VoucherRadius data")

    val aggregatesWriter = new AggregatesWriter(radiusCreditDailyData, voucherRadiusData, outputConf = settings.output)
    aggregatesWriter.writeOutput()

    logger.info("Preparing stage writer")
    val stageWriter = new StageWriter(radiusCreditDailyData, voucherRadiusData.voucherRadiusDaily,
      settings.referenceData.path.get+ settings.referenceData.sessionFile.get, settings.referenceData.path.get+ settings.referenceData.completeFile.get,
      settings.appParams.firstDate.get)
    stageWriter.writeOutput()


    logger.info("Generating excel reports daily")
    val excelReports = new ExcelReports(radiusCreditDailyData, voucherRadiusData.voucherRadiusDaily, settings.appParams.airlineCodesForReport.get)
    val dailyReports=(excelReports.getSessionReport(), excelReports.getVoucherReport())

    logger.info("Writing excel reports")
    val excelWriter = new ExcelReportsWriter(dailyReports._1, dailyReports._2,new DailySessionReport,
      settings.output.excelReportsPath.get,
      settings.appParams.firstDate.get )
    excelWriter.writeOutput()
*/
    logger.info("Processing DONE")
  }

}
