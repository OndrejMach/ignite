package com.tmobile.sit.ignite.rcse.stages

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.TerminalDProcessor
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * A Simple class storing the first RCSE processing step result - terminal_d
 * @param sparkSession
 * @param settings - contains path where to store the data
 */
class TerminalD(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {
    val data = new TerminalDProcessor().processData().cache()

    logger.info(s"writing TerminalD data, row count: ${data.count()}")

    data
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(settings.stage.terminalPath)
  }
}
