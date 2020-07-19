package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Wrapped class for the outputs data
 * @param client
 * @param terminal
 * @param terminalSW
 * @param initConf
 * @param initUser
 * @param activeUser
 * @param uau
 */

case class RCSEOutputs(client: DataFrame,
                       terminal: DataFrame,
                       terminalSW: DataFrame,
                       initConf: DataFrame,
                       initUser: DataFrame,
                       activeUser: DataFrame,
                       uau: DataFrame
                      )

/**
 * This class writes the output files. Takes processed stage data, adds BOM character in order to identify UTF-8 encoding and store them
 * having proper filenames.
 * @param processingDate - input data day
 * @param data - stage data used for outputs
 * @param sparkSession
 * @param settings - paths
 */

class OutputsWriter(processingDate: Date, data: RCSEOutputs)(implicit sparkSession: SparkSession, settings: Settings) extends Logger {
  //def writeCSV
  private val UTF8CHAR = "\ufeff"

  def writeData(path: String, data: DataFrame, writeHeader: Boolean = true, timestampFormat: String="yyyy-MM-dd HH:mm:ss", dateFormat: String = "yyyy-MM-dd")(implicit sparkSession: SparkSession) = {
    logger.info(s"Writing output file ${path}")
    val firstColumn = data.columns(0)
    val writer = CSVWriter(data.withColumnRenamed(firstColumn,UTF8CHAR+firstColumn), path, delimiter = "|", timestampFormat = timestampFormat, writeHeader = writeHeader, dateFormat = dateFormat)
    writer.writeData()
  }
  def writeOutputs(): Unit = {
    logger.info(s"writing client output to ${settings.output.client}")
    writeData(path = settings.output.client, data = data.client)
    logger.info(s"writing terminal output to ${settings.output.terminal}")
    writeData(path = settings.output.terminal, data = data.terminal)
    logger.info(s"writing terminalSW output to ${settings.output.terminalSW}")
    writeData(path = settings.output.terminalSW, data = data.terminalSW)
    logger.info(s"writing init conf output to ${settings.output.initConf}")
    writeData(path = settings.output.initConf, data = data.initConf)
    logger.info(s"writing init user output to ${settings.output.initUser}")
    writeData(path = settings.output.initUser, data = data.initUser)
    logger.info(s"writing active user output to ${settings.output.activeUsers}")
    writeData(path = settings.output.activeUsers, data = data.activeUser)
    logger.info(s"writing uau output to ${settings.output.uauFile}")
    writeData(path = settings.output.uauFile, data = data.uau, dateFormat = "yyyyMMdd")
  }
}
