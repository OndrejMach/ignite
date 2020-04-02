package com.tmobile.sit.ignite.inflight

import java.text.SimpleDateFormat

import com.tmobile.sit.ignite.inflight.config.Settings
import com.tmobile.sit.ignite.inflight.processing.aggregates.ExcelReports
import com.tmobile.sit.ignite.inflight.processing.writers.{ExcelReportType, ExcelReportsWriter, MonthlySessionReport}
import org.apache.spark.sql.SparkSession

class MonthlyReport(settings: Settings)(implicit sparkSession: SparkSession) extends Processor {
  override def executeCalculation(): Unit = {
    logger.info("Calculation of monthly reports started")

    logger.info(s"Report will be generated for month ${settings.appParams.monthlyReportDate.get}")
    val dataSession = sparkSession.read.parquet(settings.referenceData.path.get + settings.referenceData.sessionFile.get)
    val dataComplete = sparkSession.read.parquet(settings.referenceData.path.get + settings.referenceData.completeFile.get)
    logger.info("Data read from stage")
    logger.info("Preparing processor")
    val reportGenerator = new ExcelReports(dataSession, dataComplete,settings.appParams.airlineCodesForReport.get)
    logger.info("Generating report data")
    val reportsData = (reportGenerator.getSessionReport(), reportGenerator.getVoucherReport())
    logger.info("Writing monthly reports")
    val writer = new ExcelReportsWriter(reportsData._1, reportsData._2, new MonthlySessionReport(),
      date = settings.appParams.monthlyReportDate.get, path = settings.output.excelReportsPath.get)
    writer.writeOutput()
    logger.info("Processing DONE")
  }
}
