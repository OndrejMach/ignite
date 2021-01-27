package com.tmobile.sit.ignite.rcse.stages

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.EventsToStage
import com.tmobile.sit.ignite.rcse.writer.StageWriter
import org.apache.spark.sql.SparkSession

/**
 * This class generates all the important stage data. A bit problematic is client, terminal and terminalSW which are read and then replaced.
 * This behaviour should be revised. DM, REGDER, Conf and active users dimension are always put into appropriate partition according to the processingf date.
 * @param sparkSession
 * @param settings - paths where to store stage files.
 */

class Events(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {
    logger.info("Getting Events data")
    val eventsData = new EventsToStage(Timestamp.valueOf(LocalDateTime.now())).processData()


    logger.info(s"Writing client data, count: ${eventsData.client.count()}")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.client, settings.stage.clientPath).writeData()
    logger.info(s"Writing terminal data, count ${eventsData.terminal.count()}")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.terminal, settings.stage.terminalPath).writeData()
    logger.info(s"Writing terminalSW data, count ${eventsData.terminalSW.count()}")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.terminalSW, settings.stage.terminalSWPath).writeData()
    logger.info("Writing RegDer data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.regDer, settings.stage.regDerEvents).writeData()
    logger.info("Writing DM data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = eventsData.dm, settings.stage.dmEventsFile).writeData()

  }
}
