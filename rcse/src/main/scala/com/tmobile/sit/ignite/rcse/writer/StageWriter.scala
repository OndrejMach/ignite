package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

case class StageData(client: DataFrame,
                     terminal: DataFrame,
                     terminalSW: DataFrame,
                     regDerEvents: DataFrame,
                     dmEvents: DataFrame,
                     activeUsers: DataFrame,
                     conf: DataFrame
                    )

class StageWriter(processingDate: Date, stageData: StageData)(implicit sparkSession: SparkSession, settings: Settings) extends RCSEWriter(processingDate = processingDate) {
  def writeData(): Unit = {
    logger.info(s"Writing client data")
    writeParquet(stageData.client, settings.stage.clientPath)
    logger.info(s"Writing terminal data")
    writeParquet(stageData.terminal, settings.stage.terminalPath)
    logger.info(s"Writing terminalSW data")
    writeParquet(stageData.terminalSW, settings.stage.terminalSWPath)
    logger.info(s"Writing RegDer data")
    writeParquet(stageData.regDerEvents, settings.stage.regDerEventsToday, true)
    logger.info(s"Writing DM events data")
    writeParquet(stageData.dmEvents, settings.stage.dmEventsFile, true)
    logger.info(s"Writing conf data")
    writeParquet(stageData.conf, settings.stage.confFile, true)
    logger.info(s"Writing active users data")
    writeParquet(stageData.activeUsers, settings.stage.activeUsersToday, true)
  }
}
