package com.tmobile.sit.ignite.rcse.stages

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.TerminalDProcessor
import com.tmobile.sit.ignite.rcse.writer.TerminalDStageWriter
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * A Simple class storing the first RCSE processing step result - terminal_d
 * @param sparkSession
 * @param settings - contains path where to store the data
 */
class TerminalD(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {
    val data = new TerminalDProcessor().processData().persist(StorageLevel.MEMORY_ONLY)

    logger.info(s"writing TerminalD data, row count: ${data.count()}")

    new TerminalDStageWriter(processingDate = settings.app.processingDate, data = data).writeData()

  }
}
