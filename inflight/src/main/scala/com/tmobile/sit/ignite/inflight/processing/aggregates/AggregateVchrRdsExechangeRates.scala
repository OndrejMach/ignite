package com.tmobile.sit.ignite.inflight.processing.aggregates

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.data.NormalisedExchangeRates
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Calculates voucher radius daily file
 * @param interimData - data required by the calculation - common with voucher radius full file
 * @param normalisedExchangeRates - exchange rates for calculation of the price in EUR
 * @param sparkSession - obvious ;)
 */

class AggregateVchrRdsExechangeRates(interimData: AggregVchrRadiusInterimData, normalisedExchangeRates: NormalisedExchangeRates)
                                    (implicit sparkSession: SparkSession) extends Logger {

  val voucherRadiusDaily: DataFrame = {
    val translate = sparkSession.udf.register("translateSeconds", translateSeconds)

    val filtered = interimData
      .joinedOrderDBVoucherAndFlightLeg
      .filter(col("voucher_type").isNotNull) // get only voucher users

    filtered.show(false)

    normalisedExchangeRates
      .joinWithExchangeRates(filtered)
      .withColumnRenamed("payid", "wlan_pay_id")
      .withColumnRenamed("card_institute", "wlan_card_institute")
      .withColumnRenamed("payment_method", "wlan_payment_method")
      .withColumnRenamed("voucher_type", "wlan_voucher_type")
      .withColumnRenamed("voucher_duration", "wlan_voucher_duration")
      .withColumn("wlif_num_sessions", col("count_sessions"))
      .withColumn("wlif_session_time", translate(col("wlif_session_time")))
      .withColumn("wlif_session_volume", round(col("wlif_session_volume"), 2))
      .withColumn("amount_incl_vat", round(col("amount_incl_vat"), 2))
      .withColumn("amount_excl_vat", round(col("amount_excl_vat"), 2))
  }
}
