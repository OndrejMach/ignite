package com.tmobile.sit.ignite.rcse.processors.events

import java.sql.Date

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, when, max, monotonically_increasing_id}
import org.apache.spark.sql.types.{IntegerType, LongType}

class TerminalDimension(enrichedEvents: DataFrame, oldTerminal: DataFrame, tacData: DataFrame, load_date: Date)(implicit sparkSession: SparkSession) extends Logger{

  val newTerminal = {
    import sparkSession.implicits._
    logger.info("Getting old terminals")
    val maxID = oldTerminal.select(max("rcse_terminal_id").cast(IntegerType)).collect()(0).getInt(0)

    val dimensionBOld =
      enrichedEvents
        .filter($"rcse_terminal_id".isNotNull)
        .join(
          oldTerminal.select($"tac_code".as("tac_code_lkp"), $"terminal_id".as("terminal_id_lkp"), $"rcse_terminal_id"), Seq("rcse_terminal_id"), "left_outer").cache()
        .filter($"tac_code_lkp".isNull && $"terminal_id_lkp".isNull && $"tac_code".isNotNull)
        .select(
          $"rcse_terminal_id",
          $"tac_code",
          lit(null).as("terminal_id"),
          $"terminal_vendor".as("rcse_terminal_vendor_sdesc"),
          $"terminal_vendor".as("rcse_terminal_vendor_ldesc"),
          $"terminal_model".as("rcse_terminal_model_sdesc"),
          $"terminal_model".as("rcse_terminal_model_ldesc"),
          lit(load_date).as("modification_date")
        )


    logger.info("From the input data getting potentially new terminals")
    val dimensionBNew =
      enrichedEvents
        .filter($"rcse_terminal_id".isNull)
        .withColumn("rcse_terminal_id", lit(-1).cast(IntegerType))
        .join(tacData.select("manufacturer", "model", "terminal_id", "load_date"), Seq("terminal_id"), "left_outer")
        .withColumn("tac_code", when($"terminal_id".isNull, $"tac_code"))
        .withColumn("rcse_terminal_vendor_sdesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
        .withColumn("rcse_terminal_vendor_ldesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
        .withColumn("rcse_terminal_model_sdesc", when($"terminal_id".isNotNull, $"model").otherwise($"terminal_model"))
        .withColumn("rcse_terminal_model_ldesc", when($"terminal_id".isNotNull, $"model").otherwise($"terminal_model"))
        .select(
          lit(-1).cast(IntegerType).as("rcse_terminal_id"),
          $"tac_code",
          $"terminal_id",
          $"rcse_terminal_vendor_sdesc",
          $"rcse_terminal_vendor_ldesc",
          $"rcse_terminal_model_sdesc",
          $"rcse_terminal_model_ldesc",
          lit(load_date).as("modification_date")
        )
      .withColumn("rcse_terminal_id", (monotonically_increasing_id() + maxID).cast(IntegerType))


    val cols = dimensionBOld.columns.map(i => i + "_old")

    logger.info("merging all together to get new terminals")
    oldTerminal
      //.drop("entry_id", "load_date")
      .union(dimensionBNew)
      .join(dimensionBOld.toDF(cols: _*), $"rcse_terminal_id" === $"rcse_terminal_id_old", "left_outer")
      .withColumn("tac_code", when($"tac_code".isNull, $"tac_code_old").otherwise($"tac_code"))
      .withColumn("terminal_id", when($"terminal_id".isNull, $"terminal_id_old").otherwise($"terminal_id"))
      .withColumn("rcse_terminal_vendor_sdesc", when($"rcse_terminal_vendor_sdesc".isNull, $"rcse_terminal_vendor_sdesc_old").otherwise($"rcse_terminal_vendor_sdesc"))
      .withColumn("rcse_terminal_vendor_ldesc", when($"rcse_terminal_vendor_ldesc".isNull, $"rcse_terminal_vendor_ldesc_old").otherwise($"rcse_terminal_vendor_ldesc"))
      .withColumn("rcse_terminal_model_sdesc", when($"rcse_terminal_model_sdesc".isNull, $"rcse_terminal_model_sdesc_old").otherwise($"rcse_terminal_model_sdesc"))
      .withColumn("rcse_terminal_model_ldesc", when($"rcse_terminal_model_ldesc".isNull, $"rcse_terminal_model_ldesc_old").otherwise($"rcse_terminal_model_ldesc"))
      .withColumn("modification_date", when($"modification_date".isNull, $"modification_date_old").otherwise($"modification_date"))
      .select(
        "rcse_terminal_id",
        "tac_code",
        "terminal_id",
        "rcse_terminal_vendor_sdesc",
        "rcse_terminal_vendor_ldesc",
        "rcse_terminal_model_sdesc",
        "rcse_terminal_model_ldesc",
        "modification_date"
      )
  }
}
