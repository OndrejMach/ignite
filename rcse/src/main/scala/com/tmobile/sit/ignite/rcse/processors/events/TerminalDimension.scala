package com.tmobile.sit.ignite.rcse.processors.events

import java.sql.Date

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, when, max, first, sha2, concat, col}
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Logic for updating the terminal dimension. In case a new terminal appears in the input data it is added to the termianl file.
 *
 * @param enrichedEvents - preprocessed input events
 * @param oldTerminal    - actual terminal data
 * @param tacData        - tac data for potential enrichment
 * @param load_date      - date is added as a modification data
 * @param sparkSession
 */

class TerminalDimension(enrichedEvents: DataFrame, oldTerminal: DataFrame, tacData: DataFrame, load_date: Date)(implicit sparkSession: SparkSession) extends Logger {

  val newTerminal = {
    import sparkSession.implicits._
    logger.info("Getting old terminals")
    //val maxID = oldTerminal.select(max("rcse_terminal_id").cast(IntegerType)).collect()(0).getInt(0)

    val dimensionBOld =
      enrichedEvents
        .filter($"rcse_terminal_id".isNotNull)
        .join(
          oldTerminal.select($"tac_code".as("tac_code_lkp"), $"terminal_id".as("terminal_id_lkp"), $"rcse_terminal_id"), Seq("rcse_terminal_id"), "left_outer")
        .filter($"tac_code_lkp".isNull && $"terminal_id_lkp".isNull && $"tac_code".isNotNull)
        .select(
          $"rcse_terminal_id",
          $"tac_code",
          lit(null).as("terminal_id"),
          $"terminal_vendor".as("rcse_terminal_vendor_sdesc"),
          $"terminal_vendor".as("rcse_terminal_vendor_ldesc"),
          $"terminal_model".as("rcse_terminal_model_sdesc"),
          $"terminal_model".as("rcse_terminal_model_ldesc")
        //  lit(load_date).as("modification_date")
        )


    logger.info("From the input data getting potentially new terminals")
    val filtered = enrichedEvents
      .filter($"rcse_terminal_id".isNull)
      .join(tacData.select("manufacturer", "model", "terminal_id", "load_date"), Seq("terminal_id"), "left_outer")
      .withColumn("tac_code", when($"terminal_id".isNull, $"tac_code"))
      .withColumn("rcse_terminal_vendor_sdesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
      .withColumn("rcse_terminal_vendor_ldesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
      .withColumn("rcse_terminal_model_sdesc", when($"terminal_id".isNotNull, $"model").otherwise($"terminal_model"))
      .withColumn("rcse_terminal_model_ldesc", when($"terminal_id".isNotNull, $"model").otherwise($"terminal_model"))

    //filtered.filter("rcse_terminal_model_sdesc = 'DSB-0220'").show(false)


    val dimensionBNew =
      filtered
        //.withColumn("rcse_terminal_id", lit(-1).cast(IntegerType))
        .groupBy("rcse_terminal_vendor_sdesc", "rcse_terminal_model_sdesc", "terminal_id", "tac_code")
        .agg(
          //first("rcse_terminal_id").as("rcse_terminal_id"),
          first("rcse_terminal_vendor_ldesc").as("rcse_terminal_vendor_ldesc"),
          first("rcse_terminal_model_ldesc").as("rcse_terminal_model_ldesc")
        )
        //.withColumn("rcse_terminal_id", (monotonically_increasing_id() + maxID).cast(IntegerType))
        .withColumn("rcse_terminal_id", sha2(
          concat(c($"tac_code"), c($"terminal_id"), c($"rcse_terminal_vendor_sdesc"), c($"rcse_terminal_vendor_ldesc"),
            c($"rcse_terminal_model_sdesc"), c($"rcse_terminal_model_ldesc")), 256))
          .select(oldTerminal.columns.map(col(_)) :_*)


   // dimensionBNew.filter("rcse_terminal_model_sdesc = 'DSB-0220'").show(false)


    val cols = dimensionBOld.columns.map(i => i + "_old")

    logger.info("merging all together to get new terminals")

    //oldTerminal.printSchema()
    //dimensionBNew.printSchema()
    val ret = oldTerminal
      .union(dimensionBNew)
      .join(dimensionBOld.toDF(cols: _*), $"rcse_terminal_id" === $"rcse_terminal_id_old", "left_outer")
      .withColumn("tac_code", when($"tac_code".isNull, $"tac_code_old").otherwise($"tac_code"))
      .withColumn("terminal_id", when($"terminal_id".isNull, $"terminal_id_old").otherwise($"terminal_id"))
      .withColumn("rcse_terminal_vendor_sdesc", when($"rcse_terminal_vendor_sdesc".isNull, $"rcse_terminal_vendor_sdesc_old").otherwise($"rcse_terminal_vendor_sdesc"))
      .withColumn("rcse_terminal_vendor_ldesc", when($"rcse_terminal_vendor_ldesc".isNull, $"rcse_terminal_vendor_ldesc_old").otherwise($"rcse_terminal_vendor_ldesc"))
      .withColumn("rcse_terminal_model_sdesc", when($"rcse_terminal_model_sdesc".isNull, $"rcse_terminal_model_sdesc_old").otherwise($"rcse_terminal_model_sdesc"))
      .withColumn("rcse_terminal_model_ldesc", when($"rcse_terminal_model_ldesc".isNull, $"rcse_terminal_model_ldesc_old").otherwise($"rcse_terminal_model_ldesc"))
      //  .withColumn("modification_date", when($"modification_date".isNull, $"modification_date_old").otherwise($"modification_date"))
      .select(
        "rcse_terminal_id",
        "tac_code",
        "terminal_id",
        "rcse_terminal_vendor_sdesc",
        "rcse_terminal_vendor_ldesc",
        "rcse_terminal_model_sdesc",
        "rcse_terminal_model_ldesc"
        // "modification_date"
      )
      .groupBy("rcse_terminal_id", "tac_code", "terminal_id")
      .agg(
        //first("tac_code").alias("tac_code"),
        //first("terminal_id").alias("terminal_id"),
        first("rcse_terminal_vendor_sdesc").alias("rcse_terminal_vendor_sdesc"),
        first("rcse_terminal_vendor_ldesc").alias("rcse_terminal_vendor_ldesc"),
        first("rcse_terminal_model_sdesc").alias("rcse_terminal_model_sdesc"),
        first("rcse_terminal_model_ldesc").alias("rcse_terminal_model_ldesc")
        // max("modification_date").alias("modification_date")
      )
    ret
  }
}
