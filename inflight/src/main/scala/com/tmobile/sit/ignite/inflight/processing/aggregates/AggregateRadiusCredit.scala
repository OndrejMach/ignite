package com.tmobile.sit.ignite.inflight.processing.aggregates

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.data.NormalisedExchangeRates
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, round}

class AggregateRadiusCredit(data: AggregateRadiusCreditData, normalisedExchangeRates: NormalisedExchangeRates)(implicit sparkSession: SparkSession) extends Logger {
  private def aggregateRadiusVoucher() : DataFrame = {
    //data.filterAggrRadius.show(false)

    logger.debug(s"Voucher count: ${data.mapVoucher.select("wlif_username").distinct().count()}")
    logger.debug(s"Aggregated radius: ${data.filterAggrRadius.select("wlif_username").distinct().count()}")

    data.filterAggrRadius
      .drop("wlif_realm_code")
      .join(data.mapVoucher, Seq("wlif_username"), "inner")
      /*
      .select(
        "wlif_session_start",
        "wlif_session_stop",
        "wlif_aircraft_code",
        "wlif_flight_id",
        "wlif_airline_code",
        "wlif_flight_number",
        "wlif_airport_code_origin",
        "wlif_airport_code_destination",
        "wlif_user",
        "wlif_username",
        "wlan_username",
        "wlif_realm_code",
        "wlan_hotspot_ident_code",
        "wlif_xid_pac",
        "wlan_ta_id",
        "wlif_session_time",
        "wlif_session_volume",
        "count_sessions"
      )

       */
  }

  private def joinWithOrderDB(radiusWithVoucher: DataFrame) = {
    radiusWithVoucher
      .join(data.filterOrderDB, radiusWithVoucher("wlan_username")===data.filterOrderDB("username"), "inner")
      /*
      .select(
        "wlif_user",
        "wlan_username",
        "wlif_realm_code",
        "wlan_hotspot_ident_code",
        "wlif_aircraft_code",
        "wlif_flight_id",
        "wlif_airline_code",
        "wlif_flight_number",
        "wlif_airport_code_origin",
        "wlif_airport_code_destination",
        "wlif_session_start",
        "wlif_session_stop",
        "wlif_session_time",
        "wlif_session_volume",
        "count_sessions",
        "wlan_ta_id",
        "payid",
        "amount",
        "currency",
        "card_institute",
        "vat",
        "payment_method",
        "voucher_type",
        "voucher_duration",
        "ta_request_date",
        "wlif_username",
        "wlif_xid_pac"
      )

       */

  }
private def joinWithExchangeRates(withOrderDB: DataFrame) = {
  normalisedExchangeRates.joinWithExchangeRates(withOrderDB)
}

  def executeProcessing() : DataFrame = {
    val translate = sparkSession.udf.register("translateSeconds",translateSeconds)

    //join radius with map voucher
    logger.debug(s"COUNT RADIUS AGGREGATED: ${data.filterAggrRadius.count()}")
    val radiusWithVoucher = aggregateRadiusVoucher()
    logger.debug(s"COUNT RADIUSWITHVOUCHER: ${radiusWithVoucher.count()}")
    //radiusWithVoucher.show(false)
    //join with orderDB
    val withOrderDB = joinWithOrderDB(radiusWithVoucher)
    logger.debug(s"COUNT RADIUSWITHVOUCHER with ORDERDB: ${withOrderDB.count()}")
    //withOrderDB.show(false)
    //joinWithExchangeRates
    val withExRts = joinWithExchangeRates(withOrderDB)
      .withColumnRenamed("count_sessions", "wlif_num_sessions")
    logger.debug(s"COUNT RADIUSWITHVOUCHER with ORDERDB with ExchangeRates: ${withExRts.count()}")

    withExRts
      .withColumn("wlif_session_time",translate(col("wlif_session_time")))
      .withColumn("wlif_session_volume", round(col("wlif_session_volume"), 2))
      .withColumn("amount_incl_vat", round(col("amount_incl_vat"), 2))
      .withColumn("amount_excl_vat", round(col("amount_excl_vat"), 2))
  }

}
