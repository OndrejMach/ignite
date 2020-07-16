package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.common.Logger
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

  def writeOutputs(): Unit = {

  }
}
