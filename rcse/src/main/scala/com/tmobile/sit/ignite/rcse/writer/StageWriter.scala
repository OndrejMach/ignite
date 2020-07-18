package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class creates all stage files used for data outputs generation. It is a generic class storing just a single table
 * @param processingDate - date for which data is processed
 * @param stageData - data for a certain table
 * @param path - where to store it
 * @param partitioned - information whether data should be partitioned by processing date or not
 * @param sparkSession
 */

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
