package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Writing Terminal_D file - list of RCSE terminals
 * @param processingDate date for which data is processed
 * @param data data itself in dataframe
 * @param sparkSession of course
 * @param settings application parameters containing paths where to store files
 */

class TerminalDStageWriter(processingDate: Date, data: DataFrame)(implicit sparkSession: SparkSession, settings: Settings) extends RCSEWriter (processingDate){
  override def writeData(): Unit = {
    logger.info("Writing terminalD data")
    writeParquet(data,settings.stage.terminalPath)
  }
}
