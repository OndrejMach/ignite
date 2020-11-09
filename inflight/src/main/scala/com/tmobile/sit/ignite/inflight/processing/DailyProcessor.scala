package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.config.Settings
import com.tmobile.sit.ignite.inflight.processing.aggregates.ExcelReports
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, NormalisedExchangeRates, ReferenceData, StageData}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class holds all the daily outputs.
 * @param fullOutputs - data for full outputs
 * @param radiusCredit - radius credit aggregates
 * @param voucherRadiusOutputs - voucher radius data - both daily and full
 * @param excelSessionReport - excel daily session report
 * @param excelVoucherReport - excel daily complete report
 */
case class InflightOutputs(fullOutputs: FullOutputs, radiusCredit: DataFrame, voucherRadiusOutputs: VoucherRadiusOutputs, excelSessionReport: DataFrame, excelVoucherReport: DataFrame )

/**
 * Daily processor trait
 */
trait DailyProcessor extends Logger {
  def runDailyProcessing() :  InflightOutputs
}

/**
 * class doing daily prococessings
 * @param settings - configuration
 * @param inputFiles - input data
 * @param refData - reference (stage) data - orderDB, map voucher, exchange rates
 * @param sparkSession - yeah
 */
class DailyProcessorImpl(settings: Settings, inputFiles: InputData, refData: ReferenceData )(implicit sparkSession: SparkSession)extends DailyProcessor {

  private def getFullOutputs(stageData: StageData): FullOutputs = {
    logger.info("Preparation for full-files processing")
    val fullOutput = new FullOutputsProcessor(stageData, settings.appParams.filteredAirlineCodes.get)
    logger.info("Data for full-files")

    fullOutput.generateOutput()
  }

  private def getRadiusCredit(stageData: StageData, exchangeRates: NormalisedExchangeRates): DataFrame = {
    logger.info("Preparing processor for RadiusCredit")
    val radiusCreditProcessor = new RadiusCreditDailyProcessor(refData,stageData, exchangeRates,settings.appParams.firstDate.get,
      settings.appParams.firstPlus1Date.get, settings.appParams.minRequestDate.get)

    logger.info("Creating RadiusCreditDaily data")
    radiusCreditProcessor.executeProcessing()
  }

  private def getVchrRadius(stageData: StageData, exchangeRates: NormalisedExchangeRates) = {
    logger.info("Preparing VoucherRadius processor")
    val voucherRadiusProcessor = new VoucherRadiusProcessor(stageData, refData, exchangeRates,firstDate = settings.appParams.firstDate.get,
      lastPlus1Date = settings.appParams.firstPlus1Date.get, minRequestDate = settings.appParams.minRequestDate.get)
    logger.info("Retrieving VoucherRadius data")


    voucherRadiusProcessor.getVchrRdsData()
  }

  private def getDailyExcelReports(radiusCreditDailyData: DataFrame, voucherRadiusDataDaily: DataFrame): (DataFrame, DataFrame) = {
    logger.info("Generating excel reports daily")
    val excelReports = new ExcelReports(radiusCreditDailyData, voucherRadiusDataDaily, settings.appParams.airlineCodesForReport.get)
    (excelReports.getSessionReport(), excelReports.getVoucherReport())
  }

  override def runDailyProcessing(): InflightOutputs = {
    logger.info("Preapring stage data")
    val stageData = new StageData(inputFiles)

    val fullOutputData = getFullOutputs(stageData)

    logger.info("Preparing staged reference data (Exchange Rates)")
    val exchangeRates = new NormalisedExchangeRates(exchangeRates = refData.exchangeRates, settings.appParams.minRequestDate.get)

    val radiusCreditDailyData = getRadiusCredit(stageData, exchangeRates)

    val voucherRadiusData = getVchrRadius(stageData, exchangeRates)
    logger.info("Writing VoucherRadius data")

    val dailyReports=getDailyExcelReports(radiusCreditDailyData, voucherRadiusData.voucherRadiusDaily)

    InflightOutputs(fullOutputs = fullOutputData, radiusCredit = radiusCreditDailyData.distinct(), voucherRadiusOutputs = voucherRadiusData, excelSessionReport = dailyReports._1, excelVoucherReport = dailyReports._2)
  }
}