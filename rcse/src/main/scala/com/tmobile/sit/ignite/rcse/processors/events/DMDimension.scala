package com.tmobile.sit.ignite.rcse.processors.events

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.datastructures.EventsStage
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.tmobile.sit.ignite.rcse.processors.Lookups

class DMDimension(eventInputsEnriched: DataFrame,
                  newClient: DataFrame,
                  newTerminal: DataFrame,
                  msisdn3DesLookup: DataFrame,
                  newTerminalSW: DataFrame )(implicit sparkSession: SparkSession) extends Logger {
  import sparkSession.implicits._

  val eventsDM = {
    logger.info("Getting data for the DM dimension")
    val dimensionD = eventInputsEnriched
      .select(
        EventsStage.withLookups.head, EventsStage.withLookups.tail: _*
      )

    logger.info("Preparing input data")
    val outputPrep =
      dimensionD
        .withColumn("date_id", $"date_id".cast(DateType))
        //.withColumn("msisdn", when($"msisdn".isNotNull, encoder3des($"msisdn")).otherwise(encoder3des(lit("#"))))
        .join(msisdn3DesLookup, $"msisdn" === $"number", "left_outer")
        .withColumn("msisdn", $"des")
        .drop("des", "number")
        .withColumnRenamed("rcse_client_id", "rcse_client_id_old")
        .clientLookup(newClient)
        .withColumn("rcse_client_id", when($"rcse_client_id_old".isNull, $"rcse_client_id").otherwise($"rcse_client_id_old"))
        .terminalSimpleLookup(newTerminal)
        .terminalDescLookup(newTerminal)
        .withColumn("rcse_terminal_id",
          when($"rcse_terminal_id".isNotNull, $"rcse_terminal_id")
            .otherwise(
              when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal")
                .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac")
                  .otherwise($"rcse_terminal_id_desc")
                )
            )
        )

    val outputDone = outputPrep
      .filter($"rcse_terminal_sw_id".isNotNull)
      .select(
        EventsStage.stageColumns.head, EventsStage.stageColumns.tail: _*
      )
    logger.info("Generating daily DM output")
    outputPrep
      .filter($"rcse_terminal_sw_id".isNull)
      .drop("rcse_terminal_sw_id")
      .terminalSWLookup(newTerminalSW)
      .select(
        EventsStage.stageColumns.head, EventsStage.stageColumns.tail: _*
      )
      .union(outputDone)
  }
}
