package com.tmobile.sit.ignite.inflight.processing.aggregates

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{MapVoucher, OrderDB}
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes.{FlightLeg, Radius}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

class AggregVchrRadiusInterimData(flightLeg: Dataset[FlightLeg], radius: Dataset[Radius], voucher: Dataset[MapVoucher], orderDB: Dataset[OrderDB], firstDate: Timestamp, lastPlus1Date: Timestamp) extends Logger {
  lazy val filterFlightLeg: Dataset[FlightLeg] = {
    logger.info("Filtering FlightLeg")
    flightLeg.filter(
      flightLeg("wlif_date_time_closed").isNotNull &&
        flightLeg("wlif_flightleg_status").equalTo(lit("closed")) &&
        flightLeg("wlif_num_users").gt(lit(0)) &&
        flightLeg("wlif_date_time_closed").geq(lit(firstDate)) &&
        flightLeg("wlif_date_time_closed").leq(lit(lastPlus1Date))
    )
  }

  lazy val aggregateRadius: DataFrame = {
    logger.info("Aggregating Radius data")
    radius.groupBy("wlif_username", "wlif_flight_id")
      .agg(
        min("wlif_session_start").alias("wlif_session_start"),
        max("wlif_session_stop").alias("wlif_session_stop"),
        first("wlif_aircraft_code", true).alias("wlif_aircraft_code"),
        first("wlif_airline_code", true).alias("wlif_airline_code"),
        first("wlif_flight_number", true).alias("wlif_flight_number"),
        first("wlif_airport_code_origin").alias("wlif_airport_code_origin"),
        first("wlif_airport_code_destination").alias("wlif_airport_code_destination"),
        first("wlif_user", true).alias("wlif_user"),
        first("wlif_realm_code", true).alias("wlif_realm_code"),
        first("wlan_hotspot_ident_code", true).alias("wlan_hotspot_ident_code"),
        first("wlif_xid_pac", true).alias("wlif_xid_pac"),
        first("wlif_account_type", ignoreNulls = true).alias("wlif_account_type"),
        sum("wlif_session_time").alias("wlif_session_time"),
        sum(col("wlif_in_volume") + col("wlif_out_volume")).alias("wlif_session_volume"),
        count("wlif_username").alias("count_sessions")
      )
  }

  lazy val joinRadiusWithFlightLeg: DataFrame = {
    //Timestamp.valueOf("1900-01-01 00:00:00.0")
    logger.info("Joining Radius with flightleg")
    aggregateRadius
      .join(filterFlightLeg.select("wlif_flight_id",
        "wlif_date_time_opened", "wlif_date_time_closed", "wlif_num_users", "wlif_num_sessions"),
        Seq("wlif_flight_id"), "inner")
      .withColumn("wlif_date_time_opened", when(col("wlif_date_time_opened").equalTo(lit(Timestamp.valueOf("1900-01-01 00:00:00.0"))), col("wlif_session_stop")).otherwise(col("wlif_date_time_opened")))
      .withColumn("wlif_date_time_closed", when(col("wlif_date_time_opened").equalTo(lit(Timestamp.valueOf("1900-01-01 00:00:00.0"))), col("wlif_session_stop")).otherwise(col("wlif_date_time_opened")))
      //.drop(filterFlightLeg("wlif_flight_number"), filterFlightLeg("wlif_airport_code_origin"))
      .select("wlif_flight_id",
        "wlif_flight_number",
        "wlif_airport_code_origin",
        "wlif_airport_code_destination",
        "wlif_num_users",
        "wlif_num_sessions",
        "wlif_date_time_opened",
        "wlif_date_time_closed",
        "wlif_username",
        "wlif_realm_code",
        "wlif_account_type",
        "wlan_hotspot_ident_code",
        "count_sessions",
        "wlif_airline_code",
        "wlif_session_time",
        "wlif_session_volume")
  }

  lazy val joinVoucherWithRadiusFlightLeg: DataFrame = {
    logger.info("Joining with RadiusFlightLeg")
    val maxValWlan =
      voucher.groupBy("wlan_username")
      .agg(max("wlan_request_date").alias("wlan_request_date"))

    val maxValWlif =
      voucher.groupBy("wlif_username")
        .agg(max("wlan_request_date").alias("wlan_request_date"))


    val voucherDedup = voucher
        .join(maxValWlan, Seq("wlan_request_date", "wlan_username"), "leftsemi")
        .join(maxValWlif, Seq("wlan_request_date", "wlif_username"), "leftsemi")

    joinRadiusWithFlightLeg
      .join(voucherDedup.select("wlan_ta_id","wlif_username","wlan_username" ), Seq("wlif_username"), "left")
      .select(
        "wlif_flight_id",
        "wlif_flight_number",
        "wlif_airport_code_origin",
        "wlif_airport_code_destination",
        "wlif_date_time_closed",
        "wlif_num_users",
        "wlif_num_sessions",
        "wlif_realm_code",
        "wlif_account_type",
        "wlan_hotspot_ident_code",
        "count_sessions",
        "wlan_ta_id",
        "wlan_username",
        "wlif_airline_code",
        "wlif_date_time_opened",
        "wlif_session_time",
        "wlif_session_volume"
      )
  }

  lazy val filteredOrdedDB: Dataset[OrderDB] = {
    logger.info("Filtering OrderDB")
    orderDB.filter(t => t.result_code.get == "OK" && !t.error_code.isDefined).dropDuplicates("username")
  }

  lazy val joinedOrderDBVoucherAndFlightLeg: DataFrame = {
    //unmatched - out->campaign_name = left->wlan_username;
    logger.info("JoiningOrderDB Voucher and Flightleg")
    joinRadiusWithFlightLeg.join(filteredOrdedDB, joinRadiusWithFlightLeg("wlif_username") ===filteredOrdedDB("username"), "left" )
  }

}
