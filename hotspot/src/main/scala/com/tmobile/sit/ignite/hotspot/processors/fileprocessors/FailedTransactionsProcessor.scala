package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import java.sql.Date

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.processing.{NormalisedExchangeRates, translateHours}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FailedTransactionOutput(cities: DataFrame, vouchers: DataFrame, orderDBH:DataFrame, failedTransactions: DataFrame)

/**
 * Class for failed transactions processing. It returns new City data, new vouchers, orderDBH and failed transactions. The name is a bit misleading, but thats how this was structured in EVL.
 * @param oldCitiesData - current city data
 * @param oldVoucherData - current vouchers.
 * @param wlanHotspot - current hotspot data
 * @param orderDBData - orderDB data from the MPS input
 * @param normalisedExchangeRates - normalised exchange rates
 * @param sparkSession
 * @param processingDate - date for data calculation
 */

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
    logger.debug("ORDERDB DATA: "+wlanHotspotOrderDB.count())
    logger.debug("ORDERDB DATA AND EXCHANGE RATES: "+wlanHotspotOrderDBWithExchangeRates.count())
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
