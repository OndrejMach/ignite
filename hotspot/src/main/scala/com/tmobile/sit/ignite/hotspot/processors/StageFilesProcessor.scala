package com.tmobile.sit.ignite.hotspot.processors

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.ignite.common.data.CommonTypes
import com.tmobile.sit.ignite.common.processing.NormalisedExchangeRates
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.StageFilesData
import com.tmobile.sit.ignite.hotspot.processors.fileprocessors._
import com.tmobile.sit.ignite.hotspot.writers.StageFilesWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

case class StageData(
                      sessionD: DataFrame,
                      hotspotNew: DataFrame,
                      cities: DataFrame,
                      vouchers: DataFrame,
                      failedTransactions: DataFrame,
                      orderDBH: DataFrame,
                      sessionQ: DataFrame,
                      failedLogins: DataFrame
                    )

class StageFilesProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
  override def process(): Unit = {
    import sparkSession.implicits._
    val MIN_REQUEST_DATE = Timestamp.valueOf("2017-01-01 00:00:00")

    val processingDateTime = settings.appConfig.processing_date.get.toLocalDateTime
    implicit val processinDate = Date.valueOf(LocalDate.of(processingDateTime.getYear, processingDateTime.getMonth, processingDateTime.getDayOfMonth))

    logger.info(s"Starting processing for date ${processinDate}")
    logger.info(s"Initialising stage files")
    val stageData = new StageFilesData()

    logger.info("Processing session_D data - contains SessionD and new wlan hotspot data")
    val (sessionD, hotspotNew) =
      new SessionDProcessor(cdrData = stageData.cdrData, wlanHotspotStageData = stageData.hotspotData, processinDate)
        .processData()

    logger.info(s"Processing transaction data")
    val transactionData = new FailedTransactionsProcessor(orderDBData = stageData.orderDB,
      wlanHotspot = hotspotNew,
      oldCitiesData = stageData.cityData,
      oldVoucherData = stageData.voucherData,
      normalisedExchangeRates = new NormalisedExchangeRates(stageData.exchRatesFinal.as[CommonTypes.ExchangeRates], MIN_REQUEST_DATE)).processData()

    logger.info("Processing SessionQ")
    val sessionQ = new SessionsQProcessor(
      stageData.cdr3Days,
      Timestamp.valueOf(settings.appConfig.processing_date.get.toLocalDateTime.minusHours(2))
    ).getData

    logger.info("Processing failed logins")
    val flProc = new FailedLoginProcessor(
      failedLogins = stageData.failedLogins,
      citiesData = transactionData.cities,
      hotspotData = stageData.hotspotData,
      errorCodes = stageData.loginErrorCodes)
      .getData

    logger.info("Consolidating stage data")
    val resultData = StageData(
      sessionD = sessionD,
      hotspotNew = hotspotNew,
      cities = transactionData.cities,
      vouchers = transactionData.vouchers,
      failedTransactions = transactionData.failedTransactions,
      orderDBH = transactionData.orderDBH,
      sessionQ = sessionQ,
      failedLogins = flProc
    )
   logger.info("Writing stage files to disk")
    val writer = new StageFilesWriter(resultData)
    writer.writeData()



    /*
    val data = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/sd/cptm_ta_x_wlan_session_d.2020*", header = false, delimiter = "|", dateFormat = "yyyy-MM-dd")
      .read()
      .toDF(OutputStructures.SESSION_D_OUTPUT_COLUMNS :+ "e" :+ "l": _*)

    val winaProcessor = new WinaExportsProcessor(data)
    CSVWriter(
      path = "/Users/ondrejmachacek/tmp/hotspot/out/dwh_verkehrsmd_wlan_SPARK.csv",
      delimiter = "|",
      writeHeader = true,
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      data = winaProcessor.getTCOMData
    ).writeData()

    CSVWriter(
      path = "/Users/ondrejmachacek/tmp/hotspot/out/dwh_verkehrsmd_wlan_tmd_SPARK.csv",
      delimiter = "|",
      writeHeader = true,
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      data = winaProcessor.getTMDData
    ).writeData()

     */
  }
}
