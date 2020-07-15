package com.tmobile.sit.ignite.rcse.processors.events

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.datastructures.EventsStage
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsData
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, last, length, lit, trim, upper, when}
import org.apache.spark.sql.types.DateType

import com.tmobile.sit.ignite.rcse.processors.Lookups

class RegDerDimension(inputEventsRegDer: DataFrame,msisdn3DesLookup: DataFrame, imsi3DesLookup: DataFrame, lookups: LookupsData )(implicit sparkSession: SparkSession) extends Logger {
  val regDerOutput = {
    import sparkSession.implicits._

    //logger.info("Filtering REG and DER events")
    //val regDER = inputData.dataInput.filter($"rcse_event_type" =!= lit("DM"))

    logger.info("Calaulating REGDER dimension")
    inputEventsRegDer
      .sort(asc("msisdn"), asc("rcse_event_type"), asc("date_id"))
      .groupBy(asc("msisdn"), asc("rcse_event_type"))
      .agg(
        last("date_id").alias("date_id"),
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
      // .withColumn("imsi", when($"imsi".isNotNull, encoder3des($"imsi")))
      .join(imsi3DesLookup, $"imsi" === $"number", "left_outer")
      .withColumn("imsi", $"des")
      .drop("des", "number")
      .withColumn("imei", when($"imei".isNotNull, $"imei".substr(0, 8)))
      .withColumn("client_vendor", upper($"client_vendor"))
      .withColumn("client_version", upper($"client_version"))
      .withColumn("terminal_vendor", upper($"terminal_vendor"))
      .withColumn("terminal_model", upper($"terminal_model"))
      .withColumn("terminal_sw_version", upper($"terminal_sw_version"))
      .clientLookup(lookups.client)
      .tacLookup(lookups.tac)
      .terminalLookup(lookups.terminal)
      .withColumn("rcse_terminal_id",
        when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal")
          .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac")
            .otherwise($"rcse_terminal_id_desc")
          )
      )
      .drop("rcse_terminal_id_terminal", "rcse_terminal_id_tac", "rcse_terminal_id_desc")
      .terminalSWLookup(lookups.terminalSW)
      .select(
        EventsStage.stageColumns.head, EventsStage.stageColumns.tail: _*
      )
  }


}
