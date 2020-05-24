package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.WinaReportsInputData
import com.tmobile.sit.ignite.hotspot.processors.fileprocessors.WinaExportsProcessor
import org.apache.spark.sql.{SaveMode, SparkSession}

class WinaReportsProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
  override def process(): Unit = {
    val data = new WinaReportsInputData()

    val winaProcessor = new WinaExportsProcessor(data.data)

    winaProcessor.getTCOMData.write.mode(SaveMode.Overwrite).parquet(settings.outputConfig.wina_report.get)
    winaProcessor.getTMDData.write.mode(SaveMode.Overwrite).parquet(settings.outputConfig.wina_report_tmd.get)
  }
}
