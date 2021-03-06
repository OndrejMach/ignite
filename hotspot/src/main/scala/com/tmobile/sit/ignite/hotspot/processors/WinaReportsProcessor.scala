package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.ignite.common.common.writers.CSVWriter
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.WinaReportsInputData
import com.tmobile.sit.ignite.hotspot.processors.fileprocessors.WinaExportsProcessor
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Orchestrator class for Wina reports
 * @param sparkSession
 * @param settings
 */

class WinaReportsProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
  override def process(): Unit = {
    val data = new WinaReportsInputData()
    logger.info("Preparing calculation of Wina Reports")
    val winaProcessor = new WinaExportsProcessor(data.data)

    logger.info(s"Writing TCOM data to ${settings.outputConfig.wina_report.get}")
    CSVWriter(data = winaProcessor.getTCOMData.repartition(1), path = settings.outputConfig.wina_report.get, delimiter = ";").writeData()
    logger.info(s"Writing TMD data to ${settings.outputConfig.wina_report_tmd.get}")
    CSVWriter(data = winaProcessor.getTMDData.repartition(1), path = settings.outputConfig.wina_report_tmd.get, delimiter = ";").writeData()
  }
}
