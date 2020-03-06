package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.ExchangeRates
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, max, when}

class AggregateWithExechangeRates(interimData: AggregVchrRadiusInterimData, exchangeRates: Dataset[ExchangeRates],runId: Int, loadDate: Timestamp, minDate: Timestamp ) extends Processor {

  override def executeProcessing() : DataFrame = {

    val exchangeRts = exchangeRates.filter(t => t.exchange_rate_code.get == "D" && t.valid_to.get.after(minDate))
      .withColumn("conversion", col("exchange_rate_avg")/col("faktv"))

    val exchangeRatesDefault = getDefaultExchangeRates(exchangeRts)

      interimData.joinedOrderDBVoucherAndFlightLeg.join(exchangeRts,(interimData.joinedOrderDBVoucherAndFlightLeg("ta_request_date") === exchangeRts("valid_date")) &&
        (interimData.joinedOrderDBVoucherAndFlightLeg("currency") === exchangeRts("currency_code")), "left")
        .join(exchangeRatesDefault, Seq("currency"), "left")
        .withColumn("conversion", when(col("conversion").isNull && col("conversion_default").isNotNull, col("conversion_default")).otherwise(col("conversion")))
        .na.fill(1,Seq("conversion"))
        .withColumn("amount_incl_vat", col("amount") * col("conversion") + lit(0.005))
        .withColumn("amount_excl_vat", (col("amount") * col("conversion"))/(lit(1) + col("vat") / lit(100)) + lit(0.005) )
        .withColumn("entry_id", lit(runId))
        .withColumn("load_date", lit(loadDate))
  }
}
