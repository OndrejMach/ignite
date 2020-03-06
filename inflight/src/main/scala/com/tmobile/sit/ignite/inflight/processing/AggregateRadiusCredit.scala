package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

class AggregateRadiusCredit(data: AggregateRadiusCreditData, runId: Int, loadDate: Timestamp) extends Processor {
  private def aggregateRadiusVoucher() : DataFrame = {
    data.filterAggrRadius
      .join(data.mapVoucher, Seq("wlif_username"), "inner")
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
  }

  private def joinWithOrderDB(radiusWithVoucher: DataFrame) = {
    radiusWithVoucher
      .join(data.filterOrderDB, radiusWithVoucher("wlan_username")===data.filterOrderDB("username"), "inner")
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
        "paytid",
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

  }
private def joinWithExchangeRates(withOrderDB: DataFrame) = {
  val exchangeRatesDefault = getDefaultExchangeRates(data.getExchangeRates)

  withOrderDB
    .join(data.getExchangeRates, (withOrderDB("currency") === data.getExchangeRates("currency_code")) && (withOrderDB("ta_request_date") === data.getExchangeRates("valid_date")), "left")
    .join(exchangeRatesDefault, Seq("currency"), "left")
    .withColumn("conversion", when(col("conversion").isNull && col("conversion_default").isNotNull, col("conversion_default")).otherwise(col("conversion")))
    .na.fill(1,Seq("conversion"))
    .withColumn("amount_incl_vat", col("amount") * col("conversion") + lit(0.005))
    .withColumn("amount_excl_vat", (col("amount") * col("conversion"))/(lit(1) + col("vat") / lit(100)) + lit(0.005) )
    .withColumn("entry_id", lit(runId))
    .withColumn("load_date", lit(loadDate))
}

  override def executeProcessing() : DataFrame = {
    //join radius with map voucher
    val radiusWithVoucher = aggregateRadiusVoucher()
    //join with orderDB
    val withOrderDB = joinWithOrderDB(radiusWithVoucher)
    //joinWithExchangeRates
    joinWithExchangeRates(withOrderDB)
  }

}
