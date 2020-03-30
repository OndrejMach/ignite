package com.tmobile.sit.ignite.inflight.processing.aggregates

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.ExchangeRates
import com.tmobile.sit.ignite.inflight.processing.{Processor, getDefaultExchangeRates}
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, Dataset}

class AggregateWithExechangeRates(interimData: AggregVchrRadiusInterimData, exchangeRates: Dataset[ExchangeRates], minDate: Timestamp)
                                 (implicit runId: Int, loadDate: Timestamp) extends Logger {

  val voucherRadiusDaily: DataFrame = {

    val exchangeRts = exchangeRates.filter(col("exchange_rate_code").equalTo(lit("D")) && col("valid_to").gt(lit(minDate)))
      .withColumn("conversion", col("exchange_rate_avg") / col("faktv"))
      .drop("entry_id")
      .drop("load_date")

    val exchangeRatesDefault = getDefaultExchangeRates(exchangeRts)

    //exchangeRts.printSchema()
    //interimData.joinedOrderDBVoucherAndFlightLeg.printSchema()

    val result = interimData
      .joinedOrderDBVoucherAndFlightLeg
      .filter(col("voucher_type").isNotNull) // get only voucher users
      .drop("entry_id")
      .drop("load_date")
      .join(
        exchangeRts,
        (interimData.joinedOrderDBVoucherAndFlightLeg("ta_request_date") < exchangeRts("valid_to")) &&
        (interimData.joinedOrderDBVoucherAndFlightLeg("currency") === exchangeRts("currency_code")), "left")
      .join(exchangeRatesDefault, Seq("currency"), "left")
      .withColumn("conversion", when(col("conversion").isNull && col("conversion_default").isNotNull, col("conversion_default")).otherwise(col("conversion")))
      .na.fill(1, Seq("conversion"))
      .withColumn("amount_incl_vat", col("amount") * col("conversion") + lit(0.005))
      .withColumn("amount_excl_vat", (col("amount") * col("conversion")) / (lit(1) + col("vat") / lit(100)) + lit(0.005))
      .withColumn("entry_id", lit(runId))
      .withColumn("load_date", lit(loadDate))

     // .withColumnRenamed("ta_id", "wlan_ta_id")//wlan_pay_id
      .withColumnRenamed("payid", "wlan_pay_id")
      .withColumnRenamed("card_institute", "wlan_card_institute")
      .withColumnRenamed("payment_method", "wlan_payment_method")
      .withColumnRenamed("voucher_type", "wlan_voucher_type")
      .withColumnRenamed("voucher_duration", "wlan_voucher_duration")
//        .withColumn("wlif_session_time", )

      //.withColumnRenamed("hotspot_ident_code", "wlan_hotspot_ident_code")
      //.withColumnRenamed("hotspot_ident_code", "wlan_hotspot_ident_code")
    //result.printSchema()

      result.select("wlif_date_time_opened","wlif_date_time_closed",
        "wlif_flight_id","wlif_flight_number",
        "wlif_airport_code_origin","wlif_airport_code_destination",
        "wlif_realm_code","wlif_airline_code",
        "wlif_account_type","wlan_ta_id",
        "wlan_pay_id","wlan_card_institute",
        "wlan_payment_method","wlan_voucher_type",
        "wlan_voucher_duration","wlan_hotspot_ident_code",
        "amount_incl_vat","amount_excl_vat",
        "wlif_num_sessions","wlif_session_volume",
        "wlif_session_time","campaign_name")
  }
}
