package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.data.FailedTransactionsDataStructures
import com.tmobile.sit.ignite.hotspot.processors.udfs.DirtyStuff
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class TransactionOutputs(orderDBH: DataFrame, failedTransaction: DataFrame)

/**
 * this class calculates transaction data - meand it gets orderDB data joined with hotspot and exchange rates, vouchers and cities and generates outputs which are related to financial transactions - OrderDB_H and failed transactions.
 * @param wlanHostspotOrderDBExchangeRates -orderDB data joined with hotspot and exchange rates.
 * @param voucherData - currently known vouchers
 * @param citiesData - known cities
 * @param processingDatePlus1
 * @param sparkSession
 */

class TransactionsData(wlanHostspotOrderDBExchangeRates: DataFrame, voucherData: VoucherData, citiesData: CitiesData, processingDatePlus1: Date)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  private val citiesToJoin = {
    logger.info("Preparing cities for lookup")
    citiesData
      .allCities
      .select("city_id", "city_code")
      // .withColumn("city_code", when($"city_code".equalTo("undefined"), lit("UNDEFINED")).otherwise($"city_code"))
      .sort("city_code")
      .groupBy("city_code")
      .agg(max("city_id").alias("city_id"))
    .na.fill("-1", Seq("city_code"))
  }


  private val transactionsData = {
    logger.info("Getting transactio data from WlanHostpot, orderDB and exchangeRates joined dataframe")
    val vouchers = voucherData.allVouchers
      .filter(($"valid_from" < lit(processingDatePlus1).cast(TimestampType)) && ($"valid_to" >= lit(processingDatePlus1).cast(TimestampType)))

    wlanHostspotOrderDBExchangeRates
      .join(
        vouchers.select("wlan_voucher_id", FailedTransactionsDataStructures.JOIN_COLUMNS_VOUCHER: _*),
        FailedTransactionsDataStructures.JOIN_COLUMNS_VOUCHER,
        "left_outer")
      .withColumn("reduced_amount", when($"reduced_amount".isNotNull, $"reduced_amount").otherwise($"amount"))
      .withColumn("discount_rel", concat(((($"amount" - $"reduced_amount") * 100) / round($"amount", 2)).cast(StringType), lit("%")))
      .na.fill("No Discount", Seq("campaign_name"))
  }


  private val orderDBH = {
    logger.info("Calculating orderDB_H output from OK transactions")
    val OKTransactions = transactionsData.filter($"result_code".equalTo("OK"))

    val correctPrecision = udf { l: Double => DirtyStuff.precisionCorrection(l) }
    val padPerc = udf {s: String => DirtyStuff.padPercentage(s)}

    val aggregation = OKTransactions
      .groupBy(FailedTransactionsDataStructures.KEY_AGG_ORDERDB_H.head, FailedTransactionsDataStructures.KEY_AGG_ORDERDB_H.tail: _*)
      .agg(
        first("conversion").alias("conversion"),
        count("*").alias("num_of_transactions"),
        sum("number_miles").alias("num_flight_miles"),
        sum("amount").alias("sum_amount"),
        sum("reduced_amount").alias("sum_red_amount"),
        max("ta_request_datetime").alias("request_hour")
      )//.cache()

    aggregation
      .withColumn("sum_amount_red_amount", $"sum_amount" - $"sum_red_amount")
      .withColumn("t_vat", $"vat" / lit(100) + lit(1))
      .withColumnRenamed("voucher_type", "wlan_voucher_type")
      .withColumn("amount_d_incl_vat", correctPrecision(round($"sum_amount_red_amount" * $"conversion", 2)))
      .withColumn("amount_d_excl_vat", correctPrecision(round($"sum_amount_red_amount" * $"conversion" / $"t_vat", 2)))
      .withColumn("amount_d_incl_vat_lc", correctPrecision(round($"sum_amount_red_amount", 2)))
      .withColumn("amount_d_excl_vat_lc", correctPrecision(round($"sum_amount_red_amount" / $"t_vat", 2)))
      .withColumn("amount_c_incl_vat", correctPrecision(round($"sum_red_amount" * $"conversion", 2)))
      .withColumn("amount_c_excl_vat", correctPrecision(round($"sum_red_amount" * $"conversion" / $"t_vat", 2)))
      .withColumn("amount_c_incl_vat_lc", correctPrecision(round($"sum_red_amount", 2)))
      .withColumn("amount_c_excl_vat_lc", correctPrecision(round($"sum_red_amount" / $"t_vat", 2)))
      .withColumn("amount_incl_vat", correctPrecision(round($"sum_amount" * $"conversion", 2)))
      .withColumn("amount_excl_vat", correctPrecision(round($"sum_amount" * $"conversion" / $"t_vat", 2)))
      .withColumn("amount_incl_vat_lc", correctPrecision(round($"sum_amount", 2)))
      .withColumn("amount_excl_vat_lc", correctPrecision(round($"sum_amount" / $"t_vat", 2)))
      .withColumn("discount_rel", padPerc($"discount_rel"))
      .na.fill("-1", Seq("city_code"))
      .join(citiesToJoin, Seq("city_code"), "left_outer")
  }

  private val failedTrans = {
    logger.info("Calculating failed transactions output")
    val failedTransactions = transactionsData.filter($"result_code".equalTo("KO"))


    failedTransactions
      .sort(FailedTransactionsDataStructures.KEY_AGG_FAILED_TRANSAC.head, FailedTransactionsDataStructures.KEY_AGG_FAILED_TRANSAC.tail: _*)
      .groupBy(FailedTransactionsDataStructures.KEY_AGG_FAILED_TRANSAC.head, FailedTransactionsDataStructures.KEY_AGG_FAILED_TRANSAC.tail: _*)
      .agg(
        count("*").alias("num_of_failed_transac"),
        sum("number_miles").alias("num_flight_miles"),
        max("ta_request_datetime").alias("request_hour")
      )
      .withColumn("wlan_voucher_type", $"voucher_type")
      .na.fill("-1", Seq("city_code"))
      .join(citiesToJoin, Seq("city_code"), "left_outer")
  }

  def getTransactionData() = {
    logger.info("Returning transaction data outputs")
    TransactionOutputs(orderDBH, failedTrans)
  }
}
