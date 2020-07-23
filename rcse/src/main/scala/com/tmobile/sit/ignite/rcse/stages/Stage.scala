package com.tmobile.sit.ignite.rcse.stages

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.{ActiveUsersToStage, ConfToStage, EventsToStage}
import com.tmobile.sit.ignite.rcse.writer.StageWriter
import org.apache.spark.sql.SparkSession

/**
 * This class generates all the important stage data. A bit problematic is client, terminal and terminalSW which are read and then replaced.
 * This behaviour should be revised. DM, REGDER, Conf and active users dimension are always put into appropriate partition according to the processingf date.
 * @param sparkSession
 * @param settings - paths where to store stage files.
 */

class Stage(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {
    logger.info("Getting Events data")
    val eventsData = new EventsToStage(Timestamp.valueOf(LocalDateTime.now())).processData()

    logger.info("Writing RegDer data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.regDer, settings.stage.regDerEvents, true).writeData()
    logger.info("Writing DM data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.dm, settings.stage.dmEventsFile, true).writeData()
    logger.info("Writing client data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.client, settings.stage.clientPath).writeData()
    logger.info("Writing terminal data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.terminal, settings.stage.terminalPath).writeData()
    logger.info("Writing terminalSW data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.terminalSW, settings.stage.terminalSWPath).writeData()

    logger.info("Getting Conf data")
    val conf = new ConfToStage().processData()
    logger.info("Writing Conf data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = conf, settings.stage.confFile, true).writeData()

    logger.info("Getting Active Users data")
    val activeUsers = new ActiveUsersToStage().processData()
    logger.info("Writing Conf data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = activeUsers, settings.stage.activeUsers, true).writeData()

  }
}
