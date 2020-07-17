package com.tmobile.sit.ignite.rcse.processors.terminald

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.structures.Terminal
import org.apache.spark.sql.functions.{first, lit, monotonically_increasing_id, when}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class UpdateDTerminal(terminalDData: DataFrame, tac: DataFrame, maxDate: Date )(implicit sparkSession: SparkSession) extends Logger{
  import sparkSession.implicits._


  private def getTacTerminalJoined(terminalFiltered: DataFrame, tacProcessed: DataFrame): (DataFrame, DataFrame) = {

    val joinTacTerminal =
    terminalFiltered
      .join(tacProcessed, Seq("tac_code"), "left")

    val tacNotNull =
    joinTacTerminal
      .filter($"id".isNotNull)
      .withColumn("tac_code", lit(null).cast(StringType))
      .withColumn("terminal_id", $"id")
      .withColumn("modification_date", $"load_date")
      .select("rcse_terminal_id", "tac_code",
        "rcse_terminal_vendor_sdesc", "rcse_terminal_vendor_ldesc",
        "rcse_terminal_model_sdesc", "rcse_terminal_model_ldesc",
        "modification_date", "terminal_id")


    val tacNull =
    joinTacTerminal
      .filter($"id".isNull)
      .select("rcse_terminal_id", "tac_code",
        "rcse_terminal_vendor_sdesc", "rcse_terminal_vendor_ldesc",
        "rcse_terminal_model_sdesc", "rcse_terminal_model_ldesc",
        "modification_date")
      .withColumn("terminal_id", lit(null).cast(IntegerType))
    (tacNotNull, tacNull)
  }

  private def getTerminal(tacNotNull: DataFrame,maxTerminalID: Int ) : (DataFrame, DataFrame) = {

    def tuneDF(data: DataFrame): DataFrame = {
      data
        .sort("rcse_terminal_id", "tac_code", "terminal_id", "rcse_terminal_vendor_sdesc", "rcse_terminal_model_sdesc", "modification_date")
        .groupBy("rcse_terminal_id", "tac_code", "terminal_id", "rcse_terminal_vendor_sdesc", "rcse_terminal_model_sdesc")
        .agg(
          first("modification_date").alias("modification_date"),
          first("rcse_terminal_vendor_ldesc").alias("rcse_terminal_vendor_ldesc"),
          first("rcse_terminal_model_ldesc").alias("rcse_terminal_model_ldesc")
        )
    }

    val cols = terminalDData.columns.map(_ + "_orig")
    val join2I = tacNotNull
      .join(terminalDData.toDF(cols: _*), $"rcse_terminal_id" === $"rcse_terminal_id_orig", "left")
      .withColumn("rcse_terminal_id", when(
        $"rcse_terminal_vendor_sdesc_orig".isNotNull && $"rcse_terminal_vendor_ldesc_orig".isNotNull &&
          $"rcse_terminal_model_sdesc_orig".isNotNull && $"rcse_terminal_model_ldesc_orig".isNotNull &&
          $"rcse_terminal_model_ldesc" === $"rcse_terminal_model_ldesc_orig" &&
          $"rcse_terminal_model_sdesc" === $"rcse_terminal_model_sdesc_orig",
        $"rcse_terminal_id").otherwise(when(
        $"rcse_terminal_id_orig".isNotNull, $"rcse_terminal_id_orig").otherwise(lit(-1))
      )
      )
      .select(tacNotNull.columns.head, tacNotNull.columns.tail: _*)
      val join2 = tuneDF(join2I.filter($"rcse_terminal_id" =!= lit(-1)))
        .withColumn("modification_date",when($"modification_date".isNull,lit(Date.valueOf(LocalDate.now()))).otherwise($"modification_date"))

      val nullTerminalId =
        tuneDF(join2I.filter($"rcse_terminal_id" === lit(-1)))
          .withColumn("rcse_terminal_id", monotonically_increasing_id() + lit(maxTerminalID))
          .withColumn("modification_date",when($"modification_date".isNull,lit(Date.valueOf(LocalDate.now()))).otherwise($"modification_date"))


    (join2,nullTerminalId )
    }

  def getData() : DataFrame = {
    logger.info("Initialising Terminal D data")
    val terminalD = new TerminalDData(terminalDData = terminalDData)
    logger.info("Initialising TAC data")
    val tacData = new TacData(tac = tac, maxDate = maxDate)

    logger.info("Getting data with TacCode null and valid TacCode data")
    val (tacNull, tacNotNull) = getTacTerminalJoined(terminalD.terminalFiltered, tacData.tacProcessed)

    logger.info("Getting valid data from both TerminalD and Tac Data as well as data with null terminalId")
    val (validData, nullTerminalId) = getTerminal(tacNotNull, terminalD.maxTerminalID)

    logger.info("Unioning all together")

    validData.select(Terminal.terminalDoutputCols.head, Terminal.terminalDoutputCols.tail: _*)
      .union(nullTerminalId.select(Terminal.terminalDoutputCols.head, Terminal.terminalDoutputCols.tail: _*))
      .union(tacNull.select(Terminal.terminalDoutputCols.head, Terminal.terminalDoutputCols.tail: _*))
      .union(terminalD.terminalNullTACCode.select(Terminal.terminalDoutputCols.head, Terminal.terminalDoutputCols.tail: _*))

  }
}
