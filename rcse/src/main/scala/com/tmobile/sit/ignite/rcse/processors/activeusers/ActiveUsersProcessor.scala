package com.tmobile.sit.ignite.rcse.processors.activeusers

import java.sql.Date

import com.tmobile.sit.ignite.rcse.processors.inputs.ActiveUsersInputs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{first, lit, when}

class ActiveUsersProcessor(inputs: ActiveUsersInputs, processingDate: Date)(implicit sparkSession: SparkSession) {
  import sparkSession.implicits._

  private lazy val prepEvents = inputs.inputEvents
    .select("date_id", "natco_code", "msisdn")
    .sort("msisdn")
    .groupBy("msisdn")
    .agg(first("date_id").as("date_id"), first("natco_code").as("natco_code"))


  private lazy val join1 = prepEvents
    .join(inputs.inputConf, Seq("msisdn"), "left")
    .na
    .fill(-999, Seq("rcse_tc_status_id", "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id"))
    .select(
      "date_id", "natco_code",
      "msisdn", "rcse_tc_status_id",
      "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id"
    )

  private lazy val deregisteredEvents = inputs.eventsYesterday
    .filter($"rcse_event_type" === lit("DER"))
    .select("msisdn")
    .withColumn("deregistered", lit(1))

  private lazy val join2 = inputs.activeUsersYesterday
    .join(deregisteredEvents, Seq("msisdn"), "left")
    .filter($"deregistered".isNull)


  private lazy val join2Cols = join2.columns.map(_+"_yesterday")

  lazy val result = join1
    .join(join2.toDF(join2Cols :_*), $"msisdn" === $"msisdn_yesterday", "outer")
    .withColumn("date_id", when($"date_id".isNull, lit(processingDate)).otherwise($"date_id"))
    .withColumn("natco_code", lit("TMD"))
    .withColumn("msisdn", when($"msisdn".isNull, $"msisdn_yesterday").otherwise($"msisdn"))
    .withColumn("rcse_tc_status_id", when($"rcse_tc_status_id".isNull, $"rcse_tc_status_id_yesterday").otherwise($"rcse_tc_status_id"))
    .withColumn("rcse_curr_client_id", when($"rcse_curr_client_id".isNull, $"rcse_curr_client_id_yesterday").otherwise($"rcse_curr_client_id"))
    .withColumn("rcse_curr_terminal_id", when($"rcse_curr_terminal_id".isNull, $"rcse_curr_terminal_id_yesterday").otherwise($"rcse_curr_terminal_id"))
    .withColumn("rcse_curr_terminal_sw_id", when($"rcse_curr_terminal_sw_id".isNull, $"rcse_curr_terminal_sw_id_yesterday").otherwise($"rcse_curr_terminal_sw_id"))
    .select("date_id", "natco_code", "msisdn", "rcse_tc_status_id", "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id")
}
