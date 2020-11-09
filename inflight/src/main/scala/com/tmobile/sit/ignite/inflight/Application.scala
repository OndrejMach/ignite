package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.config.{Settings, Setup}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData}
import com.tmobile.sit.ignite.inflight.processing.writers.{DailyWriterImpl, ExcelReportsWriter, ExcelReportsWriterImpl, MonthlySessionReport}
import org.apache.spark.sql.SparkSession

/**
 * here the processing is started:
 * 1) reads and validas configuration files, initialised logger - prints parameters
 * 2) initialises spark session
 * 3) based on the commandline argument (daily|monthly) decides whether to run daily calculation or only do monthly excel reports
 * 4) starts processing
 */

object Application extends Logger{

  private def prepareDailyCalculation(implicit sparkSession: SparkSession, settings: Settings): Processor = {
    logger.info("Processing started - input data gathering")
    val inputFiles = new InputData(settings.input)
    logger.info("Getting reference data from stage files")
    val refData = new ReferenceData(settings.referenceData)
    logger.info("Preparing daily calculation Writer")
    val dailyWriter= new DailyWriterImpl()

    new DailyCalculation(inputData = inputFiles, refData = refData, dailyWriter = dailyWriter)
  }

  private def prepareMonthlyCalculation(implicit sparkSession: SparkSession,settings: Settings): Processor = {
    logger.info("Calculation of monthly reports started")


    val month = settings.appParams.monthlyReportDate.get.toLocalDateTime
    logger.info(s"Report will be generated for month ${settings.appParams.monthlyReportDate.get}")
    val dataSession = sparkSession.read.option("basePath",settings.referenceData.path.get + settings.referenceData.completeFile.get).parquet(settings.referenceData.path.get + settings.referenceData.sessionFile.get + s"/year=${month.getYear}/month=${month.getMonthValue}")
    val dataComplete = sparkSession.read.option("basePath",settings.referenceData.path.get + settings.referenceData.completeFile.get).parquet(settings.referenceData.path.get + settings.referenceData.completeFile.get+ s"/year=${month.getYear}/month=${month.getMonthValue}")
    logger.info("Data read from stage")
    val writer = new ExcelReportsWriterImpl(reportType = new MonthlySessionReport(),
      date = settings.appParams.monthlyReportDate.get, path = settings.output.excelReportsPath.get)
    new MonthlyCalculation(dataSession = dataSession, dataComplete=dataComplete,excelReportsWriter = writer)
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

    logger.info("Getting SparkSession")
    implicit val settings = setup.settings
    implicit val sparkSession = getSparkSession(settings)



    val processor: Processor = args(0) match {
      case "monthly" => prepareMonthlyCalculation
      case "daily" => prepareDailyCalculation
      case _ =>new HelperProcessor
      }
    processor.executeCalculation()
  }
}
