package com.tmobile.sit.ignite.hotspot.processors

import java.sql.{Date, Timestamp}

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.common.processing.{NormalisedExchangeRates, translateHours}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions.udf

case class FailedTransactionOutput(cities: DataFrame, vouchers: DataFrame, orderDBH:DataFrame, failedTransactions: DataFrame)


class FailedTransactionsProcessor( oldCitiesData: DataFrame,oldVoucherData: DataFrame ,wlanHotspot: DataFrame, orderDBData: DataFrame,normalisedExchangeRates: NormalisedExchangeRates)
                                 (implicit sparkSession: SparkSession, processingDate: Date) extends Logger {
  import sparkSession.implicits._

  private val wlanOrderDBDataPreprocessed = new WlanAndOrderDBData(wlanHotspotData = wlanHotspot, orderDbDataActual = orderDBData)


  private  val wlanHotspotOrderDB = {
    logger.info("Preparing WlanHotspot and OrderDB common data for processing")

    val hoursToString = udf { l: Long => translateHours(l) }

    wlanOrderDBDataPreprocessed.hotspotData
      .join(wlanOrderDBDataPreprocessed.allOrderDB, $"wlan_hotspot_ident_code" === $"hotspot_ident_code", "inner")
      .na.fill("UNDEFINED", Seq("wlan_venue_type_code", "wlan_venue_code", "wlan_provider_code"))
      .withColumn("wlan_venue_type_code", when(upper($"wlan_venue_type_code").equalTo(lit("UNDEFINED")), lit("UNDEFINED")).otherwise($"wlan_venue_type_code"))
      .withColumn("wlan_venue_code", when(upper($"wlan_venue_code").equalTo(lit("UNDEFINED")), lit("UNDEFINED")).otherwise($"wlan_venue_code"))
      .withColumn("wlan_provider_code", when(upper($"wlan_provider_code").equalTo(lit("UNDEFINED")), lit("UNDEFINED")).otherwise($"wlan_provider_code"))
      .withColumn("duration", hoursToString($"voucher_duration"))
      .withColumn("wlan_voucher_code", concat_ws("_", $"voucher_type", $"natco", $"voucher_duration"))
  }

  private val wlanHotspotOrderDBWithExchangeRates = {
    logger.info("Enriching WlanHotspot and OrderDB data with Exchange Rates")
    normalisedExchangeRates.joinWithExchangeRates(wlanHotspotOrderDB.withColumnRenamed("valid_from", "wlan_valid_from").withColumnRenamed("valid_to", "wlan_valid_to"))
      .drop("valid_from", "valid_to")
      .withColumnRenamed("wlan_valid_from", "valid_from")
      .withColumnRenamed("wlan_valid_to", "valid_to")
  }


  def processData(): FailedTransactionOutput = {
    logger.info("Preparing Cities data")
    val citiesData = new CitiesData(wlanAndOrderDBData = wlanHotspotOrderDB, oldCitieData = oldCitiesData)
    logger.info("Preparing vouchers data")
    val voucherData = new VoucherData(wlanOrderDBExchangeRatesdata = wlanHotspotOrderDBWithExchangeRates, oldVoucherData = oldVoucherData )
    logger.info("Extracting transactions data")
    val transactionsData = new TransactionsData(
      wlanHostspotOrderDBExchangeRates = wlanHotspotOrderDBWithExchangeRates,
      citiesData = citiesData,
      voucherData = voucherData,
      processingDatePlus1 = Date.valueOf(processingDate.toLocalDate.plusDays(1)))
      .getTransactionData()
    logger.info("Preparing final output")
    FailedTransactionOutput(citiesData.allCities,voucherData.allVouchersForPrint,transactionsData.orderDBH,transactionsData.failedTransaction)
  }
}
