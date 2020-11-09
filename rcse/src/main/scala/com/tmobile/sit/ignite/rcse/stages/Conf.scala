package com.tmobile.sit.ignite.rcse.stages

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.ConfToStage
import com.tmobile.sit.ignite.rcse.writer.StageWriter
import org.apache.spark.sql.SparkSession

/**
 * This class generates all the important stage data. A bit problematic is client, terminal and terminalSW which are read and then replaced.
 * This behaviour should be revised. DM, REGDER, Conf and active users dimension are always put into appropriate partition according to the processingf date.
 * @param sparkSession
 * @param settings - paths where to store stage files.
 */

class Conf(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {

    logger.info("Getting Conf data")
    val conf = new ConfToStage().processData()

    logger.info("Writing Conf data")
    new StageWriter(processingDate = settings.app.processingDate, stageData = conf.coalesce(5), settings.stage.confFile).writeData()

  }
}
