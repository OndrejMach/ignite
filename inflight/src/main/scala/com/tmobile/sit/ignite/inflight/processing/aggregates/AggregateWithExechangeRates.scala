package com.tmobile.sit.ignite.inflight.processing.aggregates

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.ExchangeRates
import com.tmobile.sit.ignite.inflight.processing.{Processor, getDefaultExchangeRates}
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

    exchangeRts.printSchema()
    interimData.joinedOrderDBVoucherAndFlightLeg.printSchema()

    interimData.joinedOrderDBVoucherAndFlightLeg
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
  }
}
