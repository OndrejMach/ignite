package com.tmobile.sit.ignite.rcse.processors.events

import java.sql.Date

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, first, lit, max, sha2}
import org.apache.spark.sql.types.IntegerType

/**
 * Logic for updating the terminalSW dimension - takes preprocessed input files and in case there is an unknown SW version for a certain
 * terminal it is included in the new terminalSW data
 * @param enrichedEvents - events preprocessed
 * @param oldTerminalSW - actual terminalSW data
 * @param load_date - modification date for the new items
 * @param sparkSession
 */

class TerminalSWDimension(enrichedEvents: DataFrame, oldTerminalSW: DataFrame, load_date: Date)(implicit sparkSession: SparkSession) extends Logger {
  val newTerminalSW = {
    import sparkSession.implicits._
    logger.info("Getting current max terminalSWID")
    //val maxTerminalSWId = oldTerminalSW.select(max("rcse_terminal_sw_id").cast(IntegerType)).collect()(0).getInt(0)
    logger.info("Getting new SW version from the input data")

    val newItems = enrichedEvents
      .filter($"rcse_terminal_sw_id".isNull)
      .select(
       // lit(-1).as("rcse_terminal_sw_id"),
        $"terminal_sw_version".as("rcse_terminal_sw_desc")
      //  lit(load_date).as("modification_date")
      )
     // .sort(desc("rcse_terminal_sw_desc"), desc("modification_date"))
      .select("rcse_terminal_sw_desc")
      .distinct()
      .withColumn("rcse_terminal_sw_id",sha2($"rcse_terminal_sw_desc",256))
      .select(oldTerminalSW.columns.head, oldTerminalSW.columns.tail: _*)

    logger.info("Adding new SW version items to the terminalSW data")
    val cols  = oldTerminalSW.columns.filter(_ != "rcse_terminal_sw_id").map( i => first(i).alias(i))

      oldTerminalSW
      .union(newItems)
  }
}
