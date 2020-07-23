package com.tmobile.sit.ignite.rcse.stages

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.OutputsProcessor
import com.tmobile.sit.ignite.rcse.writer.OutputsWriter
import org.apache.spark.sql.SparkSession

/**
 * from the stage files, generates outputs which can be formarded to the QV server
 * @param settings - paths and filenames for the resulting outputs
 * @param sparkSession
 */

class Outputs(implicit settings: Settings, sparkSession: SparkSession) extends Executor {
  override def runProcessing(): Unit = {
    logger.info("Getting RCSE outputs")
    val outputs = new OutputsProcessor().getData

    logger.info("Writing outputs")
    new OutputsWriter(settings.app.processingDate, outputs).writeOutputs()
  }
}
