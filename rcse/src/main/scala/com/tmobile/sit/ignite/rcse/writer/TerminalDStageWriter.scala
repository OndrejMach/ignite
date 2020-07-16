package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

class TerminalDStageWriter(processingDate: Date, data: DataFrame)(implicit sparkSession: SparkSession, settings: Settings) extends RCSEWriter (processingDate){
  override def writeData(): Unit = {
    logger.info("Writing terminalD data")
    writeParquet(data,settings.stage.terminalPath)
  }
}
