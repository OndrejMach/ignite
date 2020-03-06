package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{ExchangeRates, MapVoucher, OrderDB}
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes.Radius
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

class AggregateRadiusCreditData(radius: Dataset[Radius], voucher: Dataset[MapVoucher], orderDB: Dataset[OrderDB], exchangeRates: Dataset[ExchangeRates], firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp) {
  lazy val filterAggrRadius: DataFrame = {
    radius.filter(r => r.wlif_username.isDefined &&
      (r.wlif_account_type.get == "credit") &&
      r.wlif_session_stop.get.after(firstDate) &&
      r.wlif_session_stop.get.before(lastPlus1Date))
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

  /*
   wlan_ta_id: Option[String],
                         wlan_request_date: Option[Timestamp],
                         wlan_username: Option[String],
                         wlif_username: Option[String],
                         wlif_realm_code: Option[String],
                         entry_id: Option[Int],
                         load_date: Option[Timestamp]
   */

  lazy val mapVoucher: DataFrame = {
    val maxVals =
      voucher
        .groupBy("wlan_username")
          .agg(
            max("wlan_request_date").alias("wlan_request_date")
          )
    voucher
      .join(maxVals, Seq("wlan_request_date"), "leftsemi")

  }

  lazy val filterOrderDB: DataFrame = {
    val maxVals = orderDB.groupBy("username")
      .agg(
        max("ta_request_date").alias("ta_request_date")
      )
    orderDB
      .filter(o =>  (o.result_code.get == "OK") && (!o.cancellation.isDefined))
      .join(maxVals,Seq("ta_request_date", "username"), "leftsemi")

  }

  lazy val getExchangeRates: DataFrame = {
    exchangeRates
    .filter(e => (e.exchange_rate_code.get == "D") &&
    e.valid_to.get.after(minRequestDate)
    )
      .withColumn("conversion", col("exchange_rate_avg")/col("faktv"))
  }


}
