package com.tmobile.sit.ignite.inflight.processing.aggregates

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.data.NormalisedExchangeRates
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.functions.{col, lit, round, when}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class AggregateVchrRdsExechangeRates(interimData: AggregVchrRadiusInterimData, minDate: Timestamp, normalisedExchangeRates: NormalisedExchangeRates)
                                    (implicit sparkSession: SparkSession) extends Logger {

  val voucherRadiusDaily: DataFrame = {
    val translate = sparkSession.udf.register("translateSeconds", translateSeconds)
    //interimData
     // .joinedOrderDBVoucherAndFlightLeg.groupBy("vat").count().show(false)

    val filtered = interimData
      .joinedOrderDBVoucherAndFlightLeg
      .filter(col("voucher_type").isNotNull) // get only voucher users
      //.drop("entry_id")
      //.drop("load_date")

    normalisedExchangeRates
      .joinWithExchangeRates(filtered)
      .withColumnRenamed("payid", "wlan_pay_id")
      .withColumnRenamed("card_institute", "wlan_card_institute")
      .withColumnRenamed("payment_method", "wlan_payment_method")
      .withColumnRenamed("voucher_type", "wlan_voucher_type")
      .withColumnRenamed("voucher_duration", "wlan_voucher_duration")
      .withColumn("wlif_num_sessions", col("count_sessions"))
      .withColumn("wlif_session_time",translate(col("wlif_session_time")) )
      .withColumn("wlif_session_volume", round(col("wlif_session_volume"), 2))
      .withColumn("amount_incl_vat", round(col("amount_incl_vat"), 2))
      .withColumn("amount_excl_vat", round(col("amount_excl_vat"), 2))
  }
}
