package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.common.data.CommonStructures
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.readers.TextReader
import org.apache.spark.sql.SparkSession

/**
 * wrapper class for stage data needed for stage file calculation
 * @param sparkSession
 * @param settings - configuration parameters
 */

class StageFilesData(implicit sparkSession: SparkSession, settings: Settings) extends Logger{
  private val processingDate = settings.appConfig.processing_date.get.toLocalDateTime
  private val processingDatePlus1 = processingDate.plusDays(1)
  private val processingDateMinus1 = processingDate.plusDays(-1)

  lazy val hotspotData = {
    logger.info(s"Reading wlan hotspot file from ${settings.stageConfig.wlan_hotspot_filename.get}")
    sparkSession
    .read
    .parquet(settings.stageConfig.wlan_hotspot_filename.get).cache() //"/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_d_wlan_hotspot")
  }

  lazy val cdrData = {
    logger.info(s"Reading cdr file from ${settings.stageConfig.wlan_cdr_file.get} year='${processingDate.getYear}',month='${processingDate.getMonthValue}',day='${processingDate.getDayOfMonth}'")
    sparkSession
      .read
      .parquet(settings.stageConfig.wlan_cdr_file.get) //"/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_q_wlan_cdr")
      .filter(s"year='${processingDate.getYear}' and  month='${processingDate.getMonthValue}' and day = '${processingDate.getDayOfMonth}'")
  }

  lazy val orderDB = {
    logger.info(s"Reading orderDB file from ${settings.stageConfig.orderDB_filename.get}")
    sparkSession
      .read
      .parquet(settings.stageConfig.orderDB_filename.get)//"/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_f_wlan_orderdb")
      .filter(s"(year='${processingDatePlus1.getYear}' or year='${processingDate.getYear}') and  " +
        s"(month='${processingDate.getMonthValue}' or month='${processingDatePlus1.getMonthValue}') and " +
        s"(day = '${processingDatePlus1.getDayOfMonth}' or day = '${processingDate.getDayOfMonth}')")
  }

  lazy val cityData =
    {
      logger.info(s"Reading City data file from ${settings.stageConfig.city_data.get}")
      CSVReader(path = settings.stageConfig.city_data.get,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/common/cptm_ta_d_city.csv",
        header = false, schema = Some(InterimDataStructures.CITY_STRUCT), delimiter = "|")
        .read()
    }

  lazy val voucherData = {//sparkSession.read.parquet("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_voucher.csv")
    logger.info(s"Reading voucher data from ${settings.stageConfig.wlan_voucher.get}")
    //sparkSession
    //    .read
    //    .parquet(settings.stageConfig.wlan_voucher.get)
    CSVReader(path = settings.stageConfig.wlan_voucher.get, header = false, schema = Some(InterimDataStructures.VOUCHER_STRUCT), delimiter = "|").read()
  }


  lazy val exchRatesFinal = {
    logger.info("Reading final Exchange rates")
    CSVReader(path = settings.stageConfig.exchange_rates_filename.get, delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss", header = false,
      schema = Some(CommonStructures.exchangeRatesStructure)).read()
  }

  lazy val cdr3Days = {
    logger.info(s"Reading 3 days of CDR data from ${settings.stageConfig.wlan_cdr_file.get}")
    sparkSession
      .read
      .parquet(settings.stageConfig.wlan_cdr_file.get)//"/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_q_wlan_cdr")
      .filter(s"(year='${processingDatePlus1.getYear}' or year='${processingDate.getYear}' or year='${processingDateMinus1.getYear}') and  " +
        s"(month='${processingDate.getMonthValue}' or month='${processingDatePlus1.getMonthValue}' or month='${processingDateMinus1.getMonthValue}') and " +
        s"(day = '${processingDatePlus1.getDayOfMonth}' or day = '${processingDate.getDayOfMonth}' or day = '${processingDateMinus1.getDayOfMonth}')")
  }

  val failedLogins = {
    logger.info(s"Reading failed login files ${settings.inputConfig.failed_login_filename.get}")
    new TextReader(path = settings.inputConfig.failed_login_filename.get).read()//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.FAILEDLOGINS.DAY.*.csv")
  }
  val loginErrorCodes =
    CSVReader(path = settings.stageConfig.login_errors.get,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_login_error.csv",
      header = false, schema = Some(ErrorCodes.loginErrorStruct),
      delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").read()



}
