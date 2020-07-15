package com.tmobile.sit.ignite.rcse.processors.conf

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.inputs.{ConfToStageInputs, LookupsData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first, lit, when}

class ConfProcessor(inputs: ConfToStageInputs, lookups: LookupsData, max_Date: Date, processing_date: Date)(implicit sparkSession: SparkSession)  extends Logger{
  import sparkSession.implicits._

  private lazy val outColumns = Seq("date_id", "natco_code",
    "msisdn", "rcse_tc_status_id",
    "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id",
    "rcse_curr_client_id", "rcse_curr_terminal_id",
    "rcse_curr_terminal_sw_id", "modification_date")

  private lazy val preprocessedTac = lookups.tac
    .filter($"valid_to" >= lit(max_Date) && $"id".isNotNull)
    .join(lookups.terminal.select("tac_code","rcse_terminal_id"), Seq("tac_code"), "left_outer")
    .withColumn("rcse_terminal_id_tac", $"rcse_terminal_id")
    .drop("rcse_terminal_id")
    .join(lookups.terminal.select($"rcse_terminal_id", $"terminal_id".as("terminal_id_terminal")),$"terminal_id_terminal" === $"id", "left_outer")
    .withColumn("rcse_terminal_id_term", $"rcse_terminal_id")
    .drop("rcse_terminal_id", "terminal_id_terminal")
    .select($"tac_code", $"id".as("terminal_id"), $"rcse_terminal_id_tac", $"rcse_terminal_id_term".as("rcse_terminal_id_term"))


  private lazy val preprocessedEvents = inputs.events
    .filter($"rcse_subscribed_status_id" === lit(1) && $"rcse_active_status_id" === lit(1))
    .join(preprocessedTac, $"rcse_terminal_id_tac" === $"rcse_terminal_id", "left_outer")
    .withColumn("rcse_curr_terminal_id", when($"rcse_terminal_id_term".isNotNull, $"rcse_terminal_id_term").otherwise($"rcse_terminal_id"))
    .withColumn("rcse_curr_terminal_sw_id", $"rcse_terminal_sw_id")
    .withColumn("modification_date", $"date_id")
    .withColumn("rcse_init_client_id", $"rcse_client_id")
    .withColumn("rcse_init_terminal_id", $"rcse_terminal_id")
    .withColumn("rcse_init_terminal_sw_id", $"rcse_terminal_sw_id")
    .withColumn("rcse_curr_client_id", $"rcse_client_id")
    .select(
      "date_id", "natco_code",
      "msisdn", "rcse_tc_status_id",
      "rcse_init_client_id", "rcse_init_terminal_id",
      "rcse_init_terminal_sw_id", "rcse_curr_client_id",
      "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id",
      "modification_date"
    )

  private lazy val conf2 = inputs.confData
    .join(
      preprocessedTac.withColumn("e", lit(1)).select("rcse_terminal_id_tac", "rcse_terminal_id_term", "e"),
      $"rcse_curr_terminal_id" === $"rcse_terminal_id_tac", "left_outer")
    .withColumn("term", $"rcse_terminal_id_term")
    .filter($"term".isNotNull && $"e" === lit(1) &&
      (($"rcse_curr_terminal_id".isNotNull && $"term" =!= $"rcse_curr_terminal_id") ||
        $"rcse_curr_terminal_id".isNull))
    .withColumn("rcse_curr_terminal_id", $"term")
    .withColumn("modification_date", lit(processing_date))
    .sort("msisdn")
    .groupBy("msisdn")
    .agg(
      first(outColumns.head).alias(outColumns.head),
      outColumns.tail.filter(_ != "msisdn").map(i => first(i).alias(i)) :_*
    )
    .select(
      outColumns.head, outColumns.tail :_*
    )


  private lazy val joinedEventsConfData = {
    val confColumns = inputs.confData.columns.map(_+"_conf")
    preprocessedEvents
      .join(
        inputs.confData
          .toDF(confColumns: _*)
          .withColumn("e", lit(1)),
        $"msisdn" === $"msisdn_conf",
        "left"
      )
  }

  private lazy val umatched = joinedEventsConfData
    .filter($"e".isNull)
    .select(
      outColumns.map(i => col(i+"_conf").as(i)) :_*
    )
    .filter($"msisdn".isNotNull)
    .persist()



  private lazy val joined = joinedEventsConfData
    .filter($"e".isNotNull)
    .withColumn("modification_date", $"date_id")
    .withColumn("date_id", $"date_id_conf")
    .withColumn("rcse_tc_status_id", $"rcse_tc_status_id_conf")
    .withColumn("rcse_init_client_id", $"rcse_init_client_id_conf")
    .withColumn("rcse_init_terminal_id", $"rcse_init_terminal_id_conf")
    .withColumn("rcse_init_terminal_sw_id", $"rcse_init_terminal_sw_id_conf")
    .select(
      outColumns.head, outColumns.tail :_*
    )

  private lazy val updJoin = {
    val tmpUpdate = joined
      .union(conf2)

    val allCols = tmpUpdate.columns.map(_+"_conf_update")
    inputs.confData
      .join(tmpUpdate.toDF(allCols :_*), $"msisdn" === $"msisdn_conf_update", "left")
      .filter($"msisdn".isNotNull)

      .withColumn("date_id", when($"date_id_conf_update".isNotNull, $"date_id_conf_update").otherwise($"date_id"))
      .withColumn("msisdn", when($"msisdn_conf_update".isNotNull, $"msisdn_conf_update").otherwise($"msisdn"))
      .withColumn("rcse_tc_status_id",
        when($"rcse_tc_status_id_conf_update".isNotNull, $"rcse_tc_status_id_conf_update")
          .otherwise($"rcse_tc_status_id"))
      .withColumn("rcse_init_client_id",
        when($"rcse_init_client_id_conf_update".isNotNull, $"rcse_init_client_id_conf_update")
          .otherwise($"rcse_init_client_id"))
      .withColumn("rcse_init_terminal_id",
        when($"rcse_init_terminal_id_conf_update".isNotNull, $"rcse_init_terminal_id_conf_update")
          .otherwise($"rcse_init_terminal_id"))
      .withColumn("rcse_init_terminal_sw_id",
        when($"rcse_init_terminal_sw_id_conf_update".isNotNull, $"rcse_init_terminal_sw_id_conf_update")
          .otherwise($"rcse_init_terminal_sw_id"))
      .withColumn("rcse_curr_client_id",
        when($"rcse_curr_client_id_conf_update".isNotNull, $"rcse_curr_client_id_conf_update")
          .otherwise($"rcse_curr_client_id"))
      .withColumn("rcse_curr_terminal_id",
        when($"rcse_curr_terminal_id_conf_update".isNotNull, $"rcse_curr_terminal_id_conf_update")
          .otherwise($"rcse_curr_terminal_id"))
      .withColumn("rcse_curr_terminal_sw_id",
        when($"rcse_curr_terminal_sw_id_conf_update".isNotNull, $"rcse_curr_terminal_sw_id_conf_update")
          .otherwise($"rcse_curr_terminal_sw_id"))
      .withColumn("modification_date",
        when($"modification_date_conf_update".isNotNull, $"modification_date_conf_update")
          .otherwise($"modification_date"))
      .select(
        outColumns.head, outColumns.tail :_*
      )
  }


  val result = updJoin.union(umatched)

}
