package com.tmobile.sit.ignite.inflight.processing.aggregates

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{MapVoucher, OrderDB}
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes.Radius
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Prepares data for Radius Credit daily aggregates
 * @param radius - radius input file
 * @param voucher - map voucher file from hotspot
 * @param orderDB - orderDB file from hotspot
 * @param firstDate - date for which calculation is done
 * @param lastPlus1Date - upper bound for date calculation - normally firstDate + 1 day (not included in the output)
 * @param minRequestDate - min date for which data is considered
 */

class AggregateRadiusCreditData(radius: Dataset[Radius], voucher: Dataset[MapVoucher], orderDB: Dataset[OrderDB], firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp) extends Logger {

  lazy val filterAggrRadius: DataFrame = {
    logger.info("Filtering and aggregating Radius")
    logger.debug(s"COUNT RADIUS: ${radius.count()}, ${firstDate}, ${lastPlus1Date}")
    val filtered = radius.filter(
      col("wlif_username").isNotNull &&
        col("wlif_account_type").equalTo(lit("credit")) &&
        col("wlif_session_stop") >= unix_timestamp(lit(firstDate)).cast("timestamp") &&
        col("wlif_session_stop") < unix_timestamp(lit(lastPlus1Date)).cast("timestamp")
    )

    logger.debug(s"COUNT RADIUS AFTER FILTER: ${filtered.count()}")
    filtered
      .groupBy("wlif_username", "wlif_flight_id")
      .agg(
        sum("wlif_session_time").alias("wlif_session_time"),
        sum(col("wlif_in_volume") + col("wlif_out_volume")).alias("wlif_session_volume"),
        count("wlif_username").alias("count_sessions"),
        min("wlif_session_start").alias("wlif_session_start"),
        max("wlif_session_stop").alias("wlif_session_stop"),
        first("wlif_aircraft_code").alias("wlif_aircraft_code"),
        first("wlif_airline_code").alias("wlif_airline_code"),
        first("wlif_flight_number").alias("wlif_flight_number"),
        first("wlif_airport_code_origin").alias("wlif_airport_code_origin"),
        first("wlif_airport_code_destination").alias("wlif_airport_code_destination"),
        first("wlif_user").alias("wlif_user"),
        first("wlif_realm_code").alias("wlif_realm_code"),
        first("wlan_hotspot_ident_code").alias("wlan_hotspot_ident_code"),
        first("wlif_xid_pac").alias("wlif_xid_pac")
      )
  }

  lazy val mapVoucher: DataFrame = {
    logger.info("Mapping voucher")
    val maxVals =
      voucher
        .groupBy("wlan_username")
        .agg(
          max("wlan_request_date").alias("wlan_request_date")
        )

    voucher
      .join(maxVals, Seq("wlan_request_date", "wlan_username"), "leftsemi")

  }

  lazy val filterOrderDB: DataFrame = {
    logger.info("Filtering OrderDB")
    val maxVals = orderDB
      .filter(o => (o.result_code.get == "OK") && (!o.cancellation.isDefined))
      .groupBy("username")
      .agg(
        max("ta_request_date").alias("ta_request_date")
      )
    orderDB
      .filter(o => (o.result_code.get == "OK") && (!o.cancellation.isDefined))
      .join(maxVals, Seq("ta_request_date", "username"), "leftsemi")

  }

}
