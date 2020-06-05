package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.ignite.inflight.config.Settings
import com.tmobile.sit.ignite.inflight.processing.aggregates.ExcelReports
import com.tmobile.sit.ignite.inflight.processing.writers.ExcelReportsWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class does monthly calculation - monthly excel reports
 * @param dataSession - data for session report
 * @param dataComplete - data for complete report
 * @param excelReportsWriter - writer for the outputs
 * @param sparkSession - yes
 * @param settings - parameters from the configuration file
 */
class MonthlyCalculation(dataSession: DataFrame, dataComplete:DataFrame, excelReportsWriter: ExcelReportsWriter)(implicit sparkSession: SparkSession, settings: Settings) extends Processor {
  override def executeCalculation(): Unit = {

    logger.info("Preparing processor")
    val reportGenerator = new ExcelReports(dataSession, dataComplete,settings.appParams.airlineCodesForReport.get)
    logger.info("Generating report data")
    val reportsData = (reportGenerator.getSessionReport(), reportGenerator.getVoucherReport())
    logger.info("Writing monthly reports")
    excelReportsWriter.writeOutput(reportsData._1, reportsData._2)
    logger.info("Processing DONE")
  }
}
