package com.tmobile.sit.ignite.inflight.processing.data

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes

/**
 * a common class for preparing exchange rates for the processing
 * @param exchangeRates - raw exchange rates
 * @param minRequestDate - date from which we read data - older rows are ignored
 */

class NormalisedExchangeRates(exchangeRates: Dataset[InputTypes.ExchangeRates], minRequestDate: Timestamp) extends Logger {
  private val normalisedExchangeRates: DataFrame = {
    logger.info("normalising exchange rates")
    val ret = exchangeRates
      .filter(col("exchange_rate_code") === lit("D") && col("valid_to") >= to_date(unix_timestamp(lit(minRequestDate)).cast("timestamp")))
      .withColumn("conversion", col("exchange_rate_avg") / col("faktv"))
      .select("currency_code", "conversion", "valid_from", "valid_to")
    logger.info(s"Normalisation done, count ${ret.count()}")
    ret
  }
  def joinWithExchangeRates(table: DataFrame): DataFrame = {

    logger.info("Joining table with normalised exchange rates")
    val res = table
      .join(normalisedExchangeRates,(table("currency")  === normalisedExchangeRates("currency_code"))
        && (table("ta_request_date") >= normalisedExchangeRates("valid_from") && table("ta_request_date") < normalisedExchangeRates("valid_to")),
        "left_outer")
      .filter(col("conversion").isNotNull)
      .withColumn("amount_incl_vat", col("amount") * col("conversion"))// + lit(0.005))
      .withColumn("amount_excl_vat", (col("amount") * col("conversion"))/(lit(1.0) + (col("vat") / lit(100.0))) ) //+ lit(0.005)
      logger.info("Join DONE")

    res
  }
}
