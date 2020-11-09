package com.tmobile.sit.ignite.rcse.stages

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsDataWrapper
import com.tmobile.sit.ignite.rcse.processors.{ActiveUsersToStage, EventsToStage}
import com.tmobile.sit.ignite.rcse.writer.StageWriter
import org.apache.spark.sql.SparkSession

/**
 * This class generates all the important stage data. A bit problematic is client, terminal and terminalSW which are read and then replaced.
 * This behaviour should be revised. DM, REGDER, Conf and active users dimension are always put into appropriate partition according to the processingf date.
 * @param sparkSession
 * @param settings - paths where to store stage files.
 */

class ActiveUsers(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {

    val activeUsers = new ActiveUsersToStage().processData()
    logger.info("Getting Active Users data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = activeUsers.repartition(5), settings.stage.activeUsers).writeData()

  }
}
