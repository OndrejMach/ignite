package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

case class RCSEOutputs(client: DataFrame,
                       terminal: DataFrame,
                       terminalSW: DataFrame,
                       initConf: DataFrame,
                       initUser: DataFrame,
                       activeUser: DataFrame,
                       uau: DataFrame
                      )

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
    logger.info(s"writing uau output to ${settings.output.uauFile}")
    writeData(path = settings.output.uauFile, data = data.uau, dateFormat = "yyyyMMdd")
  }
}
