package com.tmobile.sit.ignite.rcse.processors.events

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.rcse.processors.datastructures.EventsStage
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsDataReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, last, length, lit, trim, upper, when, max}
import org.apache.spark.sql.types.DateType

import com.tmobile.sit.ignite.rcse.processors.Lookups

/**
 * from the input events only registration and deregistration events are extracted and further processed.
 * @param inputEventsRegDer - input events - only REG or DER type
 * @param msisdn3DesLookup - 3DES encoded MSISDNs
 * @param imsi3DesLookup - 3DES encoded IMSIs
 * @param client - client for lookup
 * @param terminal - terminal for lookup
 * @param terminalSW - terminalSW for lookup
 * @param tac - tac for lookup
 * @param sparkSession
 */

class RegDerDimension(inputEventsRegDer: DataFrame,
                      msisdn3DesLookup: DataFrame,
                      imsi3DesLookup: DataFrame,
                      client: DataFrame,
                      terminal: DataFrame,
                      terminalSW: DataFrame,
                      tac: DataFrame)(implicit sparkSession: SparkSession) extends Logger {
  val regDerOutput = {
    import sparkSession.implicits._

    logger.info("Calaulating REGDER dimension")
    val ret = inputEventsRegDer

      .groupBy("msisdn","rcse_event_type")
      .agg(
        max("date_id").alias("date_id"),
        (for (i <- EventsStage.input if i != "msisdn" && i != "rcse_event_type" && i != "date_id") yield {
          last(i).alias(i)
        }): _*
      )
      .withColumn("tac_code", when($"imei".isNotNull && length($"imei") > lit(8), trim($"imei").substr(0, 8)).otherwise($"imei"))
      .withColumn("date_id", $"date_id".cast(DateType))
      .withColumn("natco_code", lit("TMD"))
      .withColumn("msisdn", when($"msisdn".isNotNull, $"msisdn").otherwise(lit("#")))
      .join(msisdn3DesLookup, $"msisdn" === $"number", "left_outer")
      .withColumn("msisdn", $"des")
      .drop("des", "number")
      .join(imsi3DesLookup, $"imsi" === $"number", "left_outer")
      .withColumn("imsi", $"des")
      .drop("des", "number")

      .withColumn("imei", when($"imei".isNotNull, $"imei".substr(0, 8)))
      .withColumn("client_vendor", upper($"client_vendor"))
      .withColumn("client_version", upper($"client_version"))
      .withColumn("terminal_vendor", upper($"terminal_vendor"))
      .withColumn("terminal_model", upper($"terminal_model"))
      .withColumn("terminal_sw_version", upper($"terminal_sw_version"))
      .clientLookup(client)
      .tacLookup(tac)
      .terminalLookup(terminal)
      .withColumn("rcse_terminal_id",
        when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal")
          .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac")
            .otherwise($"rcse_terminal_id_desc")
          )
      )
      .drop("rcse_terminal_id_terminal", "rcse_terminal_id_tac", "rcse_terminal_id_desc")
      .terminalSWLookup(terminalSW)
      .persist()

    //ret.filter("msisdn='1D0EA6E0134F8F38FBD7A81BBF507D04'").show(false)

    ret.select(
      EventsStage.stageColumns.head, EventsStage.stageColumns.tail: _*
    )
  }


}
