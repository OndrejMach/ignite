package com.tmobile.sit.ignite.inflight.processing.writers

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.tmobile.sit.common.writers.ExcelWriter
import com.tmobile.sit.ignite.inflight.datastructures.OutputStructure
import com.tmobile.sit.ignite.inflight.processing.writers.RepTypes.RepTypes

private[writers] object RepTypes extends Enumeration {
  type RepTypes = Value
  val session, voucher = Value
}

trait ExcelReportsWriter extends Logger {
  def writeOutput(sessionReport: DataFrame, voucherReport: DataFrame): Unit
}

/**
 * Excel reports writer - generates Excel files for session report and complete reports
 * @param reportType - specifies whether daily or monthly reports is generated
 * @param path - path where to store the reports
 * @param date - date used in the output filenames
 */
class ExcelReportsWriterImpl(reportType: ExcelReportType, path: String, date: Timestamp) extends ExcelReportsWriter {

  override def writeOutput(sessionReport: DataFrame, voucherReport: DataFrame): Unit = {
    logger.info("Writing excel reports "+reportType.getName)

    def writePerAirline(data: DataFrame, interimCols: Seq[String], outputCols: Seq[String], repType: RepTypes) = {
      logger.info("Getting list of airlines")
      val airlines = data
        .select("wlif_airline_code").distinct().collect().map(_.getString(0))


      for {i <- airlines} yield {
        val filename = repType match {
          case RepTypes.session => reportType.getFilenameSession(i, date)
          case RepTypes.voucher => reportType.getFilenameVoucher(i, date)
        }
        logger.info("Writing report for airline "+i + " to file "+filename)
        val df = data
          .filter(col("wlif_airline_code").equalTo(lit(i)))
          .select(interimCols.head, interimCols.tail: _*)
          .toDF(outputCols: _*)
          .repartition(1)

        ExcelWriter(df, path + filename).writeData()
      }
    }
    logger.info("Writing session reports")
    writePerAirline(sessionReport, OutputStructure.dailyReportSessionInterim, OutputStructure.dailReportSessionOutput, RepTypes.session)
    logger.info("Writing detailed reports")
    writePerAirline(voucherReport, OutputStructure.dailyReportVoucherInterim, OutputStructure.dailyReportVoucherOutput, RepTypes.voucher)

  }
}
