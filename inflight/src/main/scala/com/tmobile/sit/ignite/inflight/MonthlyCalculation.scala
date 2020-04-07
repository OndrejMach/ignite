package com.tmobile.sit.ignite.inflight

import java.text.SimpleDateFormat

import com.tmobile.sit.ignite.inflight.config.Settings
import com.tmobile.sit.ignite.inflight.processing.aggregates.ExcelReports
import com.tmobile.sit.ignite.inflight.processing.writers.{ExcelReportType, ExcelReportsWriter, MonthlySessionReport}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
