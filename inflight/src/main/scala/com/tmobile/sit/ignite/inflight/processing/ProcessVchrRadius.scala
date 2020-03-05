package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{ExchangeRates, MapVoucher, OrderDB}
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes.{FlightLeg, Radius}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

class ProcessVchrRadius(flightLeg: Dataset[FlightLeg], radius: Dataset[Radius], voucher: Dataset[MapVoucher], orderDB: Dataset[OrderDB], exchangeRates: Dataset[ExchangeRates], firstDate: Timestamp, lastPlus1Date: Timestamp, runId: Int, loadDate: Timestamp) {
  private lazy val filterFlightLeg: Dataset[FlightLeg] = {
    flightLeg.filter(
      flightLeg("wlif_date_time_closed").isNotNull &&
        flightLeg("wlif_flightleg_status").equalTo(lit("closed")) &&
        flightLeg("wlif_num_users").gt(lit(0)) &&
        flightLeg("wlif_date_time_closed").geq(lit(firstDate)) &&
        flightLeg("wlif_date_time_closed").leq(lit(lastPlus1Date))
    )
  }

  private lazy val aggregateRadius: DataFrame = {
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
        sum("wlif_session_time").alias("wlif_session_time"),
        sum(col("wlif_in_volume") + col("wlif_out_volume")).alias("wlif_session_volume"),
        count("wlif_username").alias("count_sessions")
      )
  }

  private lazy val joinRadiusWithFlightLeg: DataFrame = {
    //Timestamp.valueOf("1900-01-01 00:00:00.0")
    aggregateRadius
      .join(filterFlightLeg, Seq("wlif_flight_id"), "inner")
      .withColumn("wlif_date_time_opened", when(col("wlif_date_time_opened").equalTo(lit(Timestamp.valueOf("1900-01-01 00:00:00.0"))), col("wlif_session_stop")).otherwise(col("wlif_date_time_opened")))
      .withColumn("wlif_date_time_closed", when(col("wlif_date_time_opened").equalTo(lit(Timestamp.valueOf("1900-01-01 00:00:00.0"))), col("wlif_session_stop")).otherwise(col("wlif_date_time_opened")))
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

  private lazy val joinVoucherWithRadiusFlightLeg: DataFrame = {
    val voucherDedup = voucher
      .sort("wlan_username", "wlan_request_date")
      .dropDuplicates("wlan_username")
      .sort("wlif_username", "wlan_request_date")
      .dropDuplicates("wlif_username")

    joinRadiusWithFlightLeg
      .join(voucherDedup, Seq("wlif_username"), "left")
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

  def aggregateVoucherUsers(): DataFrame = {
    val nonVoucher = joinVoucherWithRadiusFlightLeg.filter("wlan_ta_id is null")
    val voucher = joinVoucherWithRadiusFlightLeg.filter("wlan_ta_id is not null")

    val nonVoucherAggr = nonVoucher
      .groupBy("wlif_flight_id", "wlif_date_time_closed", "wlif_airline_code", "wlif_account_type")
      .agg(
        sum("count_sessions").alias("non_voucher_sessions"),
        count("wlif_flight_id").alias("non_voucher_users"),
        first("wlif_flight_number").alias("wlif_flight_number"),
        first("wlif_date_time_opened").alias("wlif_date_time_opened"),
        first("wlif_num_users").alias("wlif_num_users"),
        first("wlif_num_sessions").alias("wlif_num_sessions"),
        first("wlif_realm_code").alias("wlif_realm_code"),
        first("wlan_hotspot_ident_code").alias("wlan_hotspot_ident_code")
      )

    val voucherAggr = voucher
      .groupBy("wlif_flight_id", "wlif_date_time_closed", "wlif_airline_code", "wlif_account_type")
      .agg(
        sum("count_sessions").alias("voucher_sessions"),
        count("wlif_flight_id").alias("voucher_users"),
        first("wlif_num_users").alias("vchr_wlif_num_users"),
        first("wlif_num_sessions").alias("vchr_wlif_num_sessions")
      )
      .withColumnRenamed("wlif_account_type", "vchr_wlif_account_type")

    voucherAggr
      .join(nonVoucherAggr, Seq("wlif_flight_id", "wlif_date_time_closed", "wlif_airline_code"), "right")
      .na
      .fill(0, Seq("vchr_voucher_sessions", "vchr_voucher_users", "vchr_wlif_num_users", "vchr_wlif_num_sessions"))
      .withColumn("flight_users", when(col("vchr_wlif_num_users").isNull && col("wlif_num_users") === lit(-1),
        col("voucher_users") + col("non_voucher_users"))
        .otherwise(col("wlif_num_users")))
      .withColumn("flight_users", when(col("wlif_num_users").isNull && col("vchr_wlif_num_users") === lit(-1),
        col("voucher_users") + col("non_voucher_users"))
        .otherwise(col("vchr_wlif_num_users")))

      .withColumn("flight_sessions", when(col("vchr_wlif_num_sessions").isNull && col("wlif_num_sessions") === lit(-1),
        col("voucher_sessions") + col("non_voucher_sessions"))
        .otherwise(col("wlif_num_sessions")))
      .withColumn("flight_sessions", when(col("wlif_num_sessions").isNull && col("vchr_wlif_num_sessions") === lit(-1),
        col("voucher_sessions") + col("non_voucher_sessions"))
        .otherwise(col("vchr_wlif_num_sessions")))

      .select("wlif_date_time_opened",
        "wlif_date_time_closed",
        "wlif_flight_id",
        "wlif_flight_number",
        "wlif_realm_code",
        "wlif_airline_code",
        "wlif_account_type",
        "wlan_hotspot_ident_code",
        "non_voucher_users",
        "non_voucher_sessions",
        "voucher_users",
        "voucher_sessions",
        "flight_users",
        "flight_sessions")
      .withColumn("entry_id", lit(runId))
      .withColumn("load_date", lit(loadDate))
  }

  private lazy val filteredOrdedDB: Dataset[OrderDB] = {
    orderDB.filter(t => t.result_code.get == "OK" && !t.error_code.isDefined).dropDuplicates("username")
  }

  private lazy val joinedOrderDBVoucherAndFlightLeg: DataFrame = {
    //unmatched - out->campaign_name = left->wlan_username;
    joinRadiusWithFlightLeg.join(filteredOrdedDB, joinRadiusWithFlightLeg("wlan_username") ===filteredOrdedDB("username"), "left" )
  }

  private def filterExchangeRates(minDate: Timestamp): DataFrame = {
    exchangeRates
      .filter(t => t.exchange_rate_code.get == "D" && t.valid_to.get.after(minDate))
      .withColumn("conversion", col("exchange_rate_avg")/col("faktv"))
  }

  def getWithExchangeRates(minDate: Timestamp): DataFrame = {
    val exchangeRts = filterExchangeRates(minDate)
    val exchangeRatesDefault = exchangeRts
      .groupBy("currency_code")
      .agg(max("valid_date").alias("valid_date"))
      .join(exchangeRts, Seq("currency_code", "valid_date"))
      .select("currency_code","conversion" )
      .withColumnRenamed("currency_code", "currency")
        .withColumnRenamed("conversion", "conversion_default")

    joinedOrderDBVoucherAndFlightLeg.join(exchangeRts,(joinedOrderDBVoucherAndFlightLeg("ta_request_date") === exchangeRts("valid_date")) &&
      (joinedOrderDBVoucherAndFlightLeg("currency") === exchangeRts("currency_code")))
      .join(exchangeRatesDefault, Seq("currency"), "left")
      .withColumn("conversion", when(col("conversion").isNull && col("conversion_default").isNotNull, col("conversion_default")).otherwise(col("conversion")))
      .na.fill(1,Seq("conversion"))
      .withColumn("amount_incl_vat", col("amount") * col("conversion") + lit(0.005))
      .withColumn("amount_excl_vat", (col("amount") * col("conversion"))/(lit(1) + col("vat") / lit(100)) + lit(0.005) )
      .withColumn("entry_id", lit(runId))
      .withColumn("load_date", lit(loadDate))
  }

}
