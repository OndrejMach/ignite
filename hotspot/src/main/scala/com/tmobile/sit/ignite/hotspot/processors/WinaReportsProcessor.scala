package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.WinaReportsInputData
import com.tmobile.sit.ignite.hotspot.processors.fileprocessors.WinaExportsProcessor
import org.apache.spark.sql.{SaveMode, SparkSession}

class WinaReportsProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
  override def process(): Unit = {
    val data = new WinaReportsInputData()
    logger.info("Preparing calculation of Wina Reports")
    val winaProcessor = new WinaExportsProcessor(data.data)

    logger.info("Writing TCOM data")
    winaProcessor.getTCOMData.repartition(1).write.mode(SaveMode.Overwrite).parquet(settings.outputConfig.wina_report.get)
    logger.info("Writing TMD data")
    winaProcessor.getTMDData.repartition(1).write.mode(SaveMode.Overwrite).parquet(settings.outputConfig.wina_report_tmd.get)
  }
}
