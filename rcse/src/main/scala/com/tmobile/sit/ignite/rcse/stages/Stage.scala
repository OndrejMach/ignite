package com.tmobile.sit.ignite.rcse.stages

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.{ActiveUsersToStage, ConfToStage, EventsToStage}
import com.tmobile.sit.ignite.rcse.writer.{StageData, StageWriter}
import org.apache.spark.sql.SparkSession

class Stage(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {
    logger.info("Getting Events data")
    val eventsData = new EventsToStage(Timestamp.valueOf(LocalDateTime.now())).processData()
    logger.info("Getting Active Users data")
    val activeUsers = new ActiveUsersToStage().processData()
    logger.info("Getting Conf data")
    val conf = new ConfToStage().processData()

    logger.info("Consolidating outputs")
    val dataToWrite = StageData(
      client = eventsData.client,
      terminal = eventsData.terminal,
      terminalSW = eventsData.terminalSW,
      regDerEvents = eventsData.regDer,
      dmEvents = eventsData.dm,
      conf = conf,
      activeUsers = activeUsers
    )
    logger.info("Writing data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = dataToWrite).writeData()
  }
}
