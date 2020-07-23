package com.tmobile.sit.ignite.rcse.stages

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.{AggregateUAU, InitConfAggregates, InitUserAggregates}
import com.tmobile.sit.ignite.rcse.writer.{AggregatesData, AggregatesWriter}
import org.apache.spark.sql.SparkSession

/**
 * main processor for Aggregates data - init conf, init user and uau. It initialises each particular processors
 * and after calculation writes the outputs.
 * @param sparkSession
 * @param settings - paths where to store the outputs
 */

class Aggregates(implicit sparkSession: SparkSession, settings: Settings) extends Executor {
  override def runProcessing(): Unit = {
    logger.info("Getting initConf data")
    val initConf = new InitConfAggregates().processData()
    logger.info("Getting initUser data")
    val initUser = new InitUserAggregates().processData()
    logger.info("Getting UAU data")
    val uau = new AggregateUAU().processData()

    logger.info("Consollidating outputs")
    val dataToWrite = AggregatesData(
      initConf = initConf,
      initUser = initUser,
      uauAggregates = uau
    )

    logger.info("Writing aggregates outputs to stage")
    new AggregatesWriter(settings.app.processingDate, dataToWrite ).writeData()
  }
}
