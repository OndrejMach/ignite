package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.MAX_DATE
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, lit, max, upper}
import org.apache.spark.sql.types.DateType

trait LookupsData {
  def client: DataFrame

  def tac: DataFrame

  def terminal: DataFrame

  def terminalSW: DataFrame
}


case class LookupsDataWrapper(client: DataFrame, tac: DataFrame, terminal: DataFrame, terminalSW: DataFrame) extends LookupsData

class LookupsDataReader(implicit sparkSession: SparkSession, settings: Settings) extends InputData(settings.app.processingDate) with LookupsData {

  import sparkSession.implicits._

  private def getActualData(dataFrame: DataFrame): DataFrame = {
    val maxDate = dataFrame.select(max("date").cast(DateType)).collect()(0).getDate(0)
    logger.info(s"Data valid for ${maxDate}")
    dataFrame
      .filter($"date" === lit(maxDate))
      .drop("date")


  }


  val client = {
    logger.info(s"Reading data from ${settings.stage.clientPath}")
    val client = sparkSession.read.parquet(settings.stage.clientPath)
    getActualData(client)

  }

  val tac = {
    logger.info(s"Reading data from ${settings.stage.tacPath}")
    val tacRaw = sparkSession.read.parquet(settings.stage.tacPath)

    val maxDate = tacRaw.select(max("load_date")).collect()(0)(0)

    val tac = tacRaw.filter($"load_date" === lit(maxDate))

    tac
      .filter($"valid_to" >= lit(MAX_DATE))
      .withColumn("terminal_id", $"id")
  }

  val terminal = {
    logger.info(s"Reading data from ${settings.stage.terminalPath}")
    val terminal = sparkSession.read.parquet(settings.stage.terminalPath)
    getActualData(terminal)
  }


  val terminalSW = {
    logger.info(s"Reading data from ${settings.stage.terminalSWPath}")
    val terminalSW =
      sparkSession.read.parquet(settings.stage.terminalSWPath)
       // .filter($"modification_date".isNotNull)
        .withColumn("rcse_terminal_sw_desc", upper($"rcse_terminal_sw_desc"))
    getActualData(terminalSW)
  }

}
