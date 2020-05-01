package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.data.FailedTransactionsDataStructures
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{concat, count, first, lit, round, sum, when}
import org.apache.spark.sql.types.StringType

case class TransactionOutputs(orderDBH: DataFrame, failedTransaction: DataFrame)

class TransactionsData(wlanHostspotOrderDBExchangeRates: DataFrame, voucherData: VoucherData, citiesData: CitiesData )(implicit sparkSession: SparkSession)extends Logger {
  import sparkSession.implicits._

  private val transactionsData = {
    logger.info("Getting transactio data from WlanHostpot, orderDB and exchangeRates joined dataframe")
    wlanHostspotOrderDBExchangeRates
      .join(
        voucherData.allVouchers.select("wlan_voucher_id", FailedTransactionsDataStructures.JOIN_COLUMNS_VOUCHER: _*),
        FailedTransactionsDataStructures.JOIN_COLUMNS_VOUCHER,
        "left_outer")
      .withColumn("reduced_amount", when($"reduced_amount".isNotNull, $"reduced_amount").otherwise($"amount"))
      .withColumn("discount_rel", concat(((($"amount" - $"reduced_amount") * 100) / round($"amount", 2)).cast(StringType), lit("%")))
      .na.fill("No Discount", Seq("campaign_name"))
      .join(citiesData.allCities.select("city_id", "city_code"), Seq("city_code"), "left_outer")
  }

  //transactionsData.show(false)

  private val orderDBH = {
    logger.info("Calculating orderDB_H output from OK transactions")
    val OKTransactions = transactionsData.filter($"result_code".equalTo("OK"))

    OKTransactions
    .withColumn("request_hour", $"ta_request_datetime")
    .groupBy(FailedTransactionsDataStructures.KEY_AGG_ORDERDB_H.head, FailedTransactionsDataStructures.KEY_AGG_ORDERDB_H.tail: _*)
    .agg(
      sum("number_miles").alias("num_flight_miles"),
      count("*").alias("num_of_transactions"),
      //tech columns
      sum("amount").alias("sum_amount"),
      sum("reduced_amount").alias("sum_red_amount"),
      sum($"amount" - $"reduced_amount").alias("sum_amount_red_amount"),
      first("conversion").alias("conversion"),
      first("city_id").alias("city_id")
    )
    .withColumn("t_vat", $"vat" / lit(100) + lit(1))
    .withColumn("wlan_voucher_type", $"voucher_type")
    .withColumn("amount_d_incl_vat", round($"sum_amount_red_amount" * $"conversion", 2))
    .withColumn("amount_d_excl_vat", round($"sum_amount_red_amount" * $"conversion" / $"t_vat", 2))
    .withColumn("amount_d_incl_vat_lc", round($"sum_amount_red_amount", 2))
    .withColumn("amount_d_excl_vat_lc", round($"sum_amount_red_amount" / $"t_vat", 2))
    .withColumn("amount_c_incl_vat", round($"sum_red_amount" * $"conversion", 2))
    .withColumn("amount_c_excl_vat", round($"sum_red_amount" * $"conversion" / $"t_vat", 2))
    .withColumn("amount_c_incl_vat_lc", round($"sum_red_amount", 2))
    .withColumn("amount_c_excl_vat_lc", round($"sum_red_amount" / $"t_vat", 2))
    .withColumn("amount_incl_vat", round($"sum_amount" * $"conversion", 2))
    .withColumn("amount_excl_vat", round($"sum_amount" * $"conversion" / $"t_vat", 2))
    .withColumn("amount_incl_vat_lc", round($"sum_amount", 2))
    .withColumn("amount_excl_vat_lc", round($"sum_amount" / $"t_vat", 2))
  }

  //orderDBH.show(false)

  private val failedTrans = {
    logger.info("Calculating failed transactions output")
    val failedTransactions = transactionsData.filter(!$"result_code".equalTo("OK"))

    failedTransactions
      .withColumn("request_hour", $"ta_request_datetime")
      .groupBy(FailedTransactionsDataStructures.KEY_AGG_FAILED_TRANSAC.head, FailedTransactionsDataStructures.KEY_AGG_FAILED_TRANSAC.tail: _*)
      .agg(
        count("*").alias("num_of_failed_transac"),
        sum("number_miles").alias("num_flight_miles"),
        first("city_id").alias("city_id")
      )
      .withColumn("wlan_voucher_type", $"voucher_type")
  }

  def getTransactionData() = {
    logger.info("Returning transaction data outputs")
    TransactionOutputs(orderDBH,failedTrans)
  }
}
