package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import org.apache.spark.sql.{DataFrame, SparkSession}


class StageWriter(processingDate: Date, stageData: DataFrame, path: String, partitioned: Boolean = false )(implicit sparkSession: SparkSession) extends RCSEWriter(processingDate = processingDate) {
  def writeData(): Unit = {
    logger.info(s"Writing  data to ${path}, partitioned ${partitioned}")
    writeParquet(stageData, path, partitioned)

    /*
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

     */
  }
}
