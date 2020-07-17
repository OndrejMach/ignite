package com.tmobile.sit.ignite.rcse.processors.events

import java.sql.Date

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, first, lit, max, monotonically_increasing_id}
import org.apache.spark.sql.types.IntegerType

class TerminalSWDimension(enrichedEvents: DataFrame, oldTerminalSW: DataFrame, load_date: Date)(implicit sparkSession: SparkSession) extends Logger {
  val newTerminalSW = {
    import sparkSession.implicits._
    logger.info("Getting current max terminalSWID")
    val maxTerminalSWId = oldTerminalSW.select(max("rcse_terminal_sw_id").cast(IntegerType)).collect()(0).getInt(0)
    logger.info("Getting new SW version from the input data")

    val newItems = enrichedEvents
      .filter($"rcse_terminal_sw_id".isNull)
      .select(
        lit(-1).as("rcse_terminal_sw_id"),
        $"terminal_sw_version".as("rcse_terminal_sw_desc"),
        lit(load_date).as("modification_date")
      )
      .sort(desc("rcse_terminal_sw_desc"), desc("modification_date"))
      .groupBy("rcse_terminal_sw_desc")
      .agg(
        first("rcse_terminal_sw_id").alias("rcse_terminal_sw_id"),
        first("modification_date").alias("modification_date")
      )
      .withColumn("rcse_terminal_sw_id", (monotonically_increasing_id() + lit(maxTerminalSWId)).cast(IntegerType))
      .select(oldTerminalSW.columns.head, oldTerminalSW.columns.tail: _*)

    logger.info("Adding new SW version items to the terminalSW data")
    oldTerminalSW.union(newItems)
  }
}
