package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.config.Settings
import com.tmobile.sit.ignite.inflight.processing.InflightOutputs
import org.apache.spark.sql.SparkSession

trait DailyWriter extends Logger {
  def writeDailyData(inflightOutputs: InflightOutputs): Unit
}


class DailyWriterImpl(implicit sparkSession: SparkSession,settings: Settings) extends DailyWriter {
  override def writeDailyData(inflightOutputs: InflightOutputs): Unit = {

    val fullOutputsWriter = new FullOutputWriter(settings.output,inflightOutputs.fullOutputs)
    logger.info("Writing full files")
    fullOutputsWriter.writeOutput()
    logger.info("Full files DONE")

    logger.info("Writing VoucherRadius data")

    val aggregatesWriter = new AggregatesWriter(inflightOutputs.radiusCredit, inflightOutputs.voucherRadiusOutputs, outputConf = settings.output)
    aggregatesWriter.writeOutput()

    logger.info("Preparing stage writer")
    val stageWriter = new StageWriter(inflightOutputs.radiusCredit, inflightOutputs.voucherRadiusOutputs.voucherRadiusDaily,
      settings.referenceData.path.get+ settings.referenceData.sessionFile.get, settings.referenceData.path.get+ settings.referenceData.completeFile.get,
      settings.appParams.firstDate.get)
    stageWriter.writeOutput()

    logger.info("Writing excel reports")
    val excelWriter = new ExcelReportsWriterImpl(
      path= settings.output.excelReportsPath.get,
      date = settings.appParams.firstDate.get,
      reportType = new DailySessionReport)
    excelWriter.writeOutput(sessionReport = inflightOutputs.excelSessionReport, voucherReport = inflightOutputs.excelVoucherReport)
    logger.info("Daily outputs DONE")

  }
}