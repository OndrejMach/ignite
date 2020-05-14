package com.tmobile.sit.ignite.hotspot

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.data.{CommonStructures, CommonTypes}
import com.tmobile.sit.ignite.common.processing.NormalisedExchangeRates
import com.tmobile.sit.ignite.hotspot.config.{OrderDBConfig, Settings}
import com.tmobile.sit.ignite.hotspot.data.{FUTURE, InterimDataStructures, OrderDBInputData, OutputStructures}
import com.tmobile.sit.ignite.hotspot.processors.{ExchangeRatesProcessor, FailedTransactionsProcessor, SessionDProcessor, SessionsQProcessor}
import com.tmobile.sit.ignite.hotspot.processors.staging.{CDRProcessor, OrderDBProcessor}
import com.tmobile.sit.ignite.hotspot.processors.udfs.DirtyStuff
import com.tmobile.sit.ignite.hotspot.readers.{ExchangeRatesReader, TextReader}
import com.tmobile.sit.ignite.hotspot.writers.{CDRStageWriter, OrderDBStageFilenames, OrderDBStageWriter}
import org.apache.spark.sql.SaveMode


object Application extends Logger{

  val MIN_REQUEST_DATE = Timestamp.valueOf("2017-01-01 00:00:00")
  val MAX_DATE = Date.valueOf("4712-12-31")

  val PROCESSING_DATE = Date.valueOf(LocalDate.now())

  val WLAN_HOTSPOT_ODATE = Date.valueOf(LocalDate.of(2020, 5, 8))

  val outputFile = "/Users/ondrejmachacek/tmp/common/exchangeRates.csv"
  val inputFile = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/CUP_exchangerates_d_20200508_1.csv.gz"
  val FILE_DATE =Date.valueOf("2020-05-09") // Date.valueOf(LocalDate.now())

  implicit val sparkSession = getSparkSession()
  implicit val processingDate = WLAN_HOTSPOT_ODATE

  def processExchangeRates() = {
    val exchangeRatesReader = new ExchangeRatesReader(inputFile)
    val oldExchangeFilesReader = CSVReader(path = outputFile, schema = Some(CommonStructures.exchangeRatesStructure), header = false, delimiter = "|")
    val oldData = oldExchangeFilesReader.read()
    CSVWriter(data = oldExchangeFilesReader.read(), path = outputFile + ".previous", delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()

    val exchangeRatesProcessor = new ExchangeRatesProcessor(exchangeRatesReader, oldExchangeFilesReader, MAX_DATE)

    val exchRatesFinal = exchangeRatesProcessor.runProcessing()

    CSVWriter(data = exchRatesFinal, path = outputFile, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()
  }

  def doStage(): Unit = {
    //OrderDB

    val settings: Settings = Settings(
      OrderDBConfig(wlanHotspotFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_hotspot.csv"),
        orderDBFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.MPS.DAY.2020050*.csv"),
        errorCodesFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_error_code.csv")
      ))

    val orderDBProcessor = new OrderDBProcessor(orderDBInputData = OrderDBInputData(settings.orderDBFiles), maxDate = FUTURE,"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/shared/lib/a.out")

    val orderdDBData = orderDBProcessor.processData()
    //CDR

    val inputFileCDR = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.CDR.DAY.2020050*.csv"
    val reader = new TextReader(inputFileCDR)

    val processor = new CDRProcessor(reader, FILE_DATE)

    val cdrData = processor.processData()
    new CDRStageWriter(path ="/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_q_wlan_cdr", data = cdrData).writeData()
    new OrderDBStageWriter(data = orderdDBData, filenames = OrderDBStageFilenames() ).writeData()
  }

  def doProcessingCore(): Unit = {

    import sparkSession.implicits._

    val hotspotData = sparkSession.read.parquet("/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_d_wlan_hotspot")
    val cdrData = sparkSession.read.parquet("/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_q_wlan_cdr").filter("year='2020' and  month='5' and day = '8'")

    val sessionD = new SessionDProcessor(cdrData = cdrData, wlanHotspotStageData = hotspotData, WLAN_HOTSPOT_ODATE).processData()

    //sessionD.wlanHotspotData.write.mode(SaveMode.Overwrite).parquet("/Users/ondrejmachacek/tmp/hotspot/cptm_ta_d_wlan_hotspot") TODO

    CSVWriter(path = "/Users/ondrejmachacek/tmp/hotspot/out/session_d.csv",
      data = sessionD.sessionD.select(OutputStructures.SESSION_D_OUTPUT_COLUMNS.head, OutputStructures.SESSION_D_OUTPUT_COLUMNS.tail :_*),
      delimiter = "|" ).writeData()

    //FailedTransactions
    val orderDB = sparkSession.read.parquet("/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_f_wlan_orderdb").filter("year='2020' and  month='5' and (day = '9' or day = '8')")
      //CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_f_wlan_orderdb.20200409.csv", schema = Some(OrderDBStructures.orderDBStruct),header = false, delimiter = "|" ).read()

    val cityData = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/common/cptm_ta_d_city.csv", header = false, schema = Some(InterimDataStructures.CITY_STRUCT), delimiter = "|").read()

    val voucherData = //sparkSession.read.parquet("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_voucher.csv")
      CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_voucher.csv", header = false, schema = Some(InterimDataStructures.VOUCHER_STRUCT), delimiter = "|").read()


    val exchRatesFinal = CSVReader(path =outputFile, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss", header = false,schema = Some(CommonStructures.exchangeRatesStructure )).read()

    val transactionData = new FailedTransactionsProcessor(orderDBData = orderDB,
      wlanHotspot = sessionD.wlanHotspotData,
      oldCitiesData = cityData,
      oldVoucherData = voucherData,
      normalisedExchangeRates= new NormalisedExchangeRates(exchRatesFinal.as[CommonTypes.ExchangeRates],MIN_REQUEST_DATE)).processData()

    CSVWriter(path="/Users/ondrejmachacek/tmp/hotspot/out/cities.csv",
      delimiter = "|",
      writeHeader = true,
      data = transactionData.cities.select(OutputStructures.CITIES_OUTPUT_COLUMNS.head,OutputStructures.CITIES_OUTPUT_COLUMNS.tail :_* ))
      .writeData()

    CSVWriter(path = "/Users/ondrejmachacek/tmp/hotspot/out/cptm_ta_d_wlan_voucher.csv",
      delimiter = "|",
      writeHeader = true,
      data = transactionData.vouchers
        .select(OutputStructures.VOUCHER_OUTPUT_COLUMNS.head, OutputStructures.VOUCHER_OUTPUT_COLUMNS.tail :_*),
      timestampFormat = "yyyy-MM-dd HH:mm:ss")
      .writeData()

    CSVWriter(path = "/Users/ondrejmachacek/tmp/hotspot/out/cptm_ta_x_wlan_failed_transac.20200508.csv",
      data = transactionData.failedTransactions.select(OutputStructures.FAILED_TRANSACTIONS_COLUMNS.head, OutputStructures.FAILED_TRANSACTIONS_COLUMNS.tail :_*),
      delimiter = "|",
      writeHeader = true,
      timestampFormat = "yyyy-MM-dd HH:mm:ss")
      .writeData()

    CSVWriter(path = "/Users/ondrejmachacek/tmp/hotspot/out/cptm_ta_x_wlan_orderdb_h.20200508.csv",
      data = transactionData.orderDBH.select(OutputStructures.ORDERDB_H_COLUMNS.head, OutputStructures.ORDERDB_H_COLUMNS.tail :_*),
      delimiter = "|",
      writeHeader = true,
      timestampFormat = "yyyy-MM-dd HH:mm:ss")
      .writeData()

    //cdrData.show(false)

    val cdr3Days = sparkSession.read.parquet("/Users/ondrejmachacek/tmp/hotspot/stage/cptm_ta_q_wlan_cdr").filter("year='2020' and  month='5' and (day = '8' or day = '7' or day= '9')")


    val res = new SessionsQProcessor(cdr3Days, Timestamp.valueOf(LocalDateTime.of(2020,5, 8, 0, 0, 0, 0)) ).getData

    CSVWriter(
      path = "/Users/ondrejmachacek/tmp/hotspot/out/cptm_ta_x_wlan_session_q.20200508.csv",
      delimiter = "|",
      writeHeader = true,
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      data = res.select(OutputStructures.SESSION_Q_COLUMNS.head, OutputStructures.SESSION_Q_COLUMNS.tail :_*)
    ).writeData()



    //res.show(false)
    /*
        val failedLoginReader = new TextReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.FAILEDLOGINS.DAY.20200411020101.csv")

        val errorCodes = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_login_error.csv", header = false, schema = Some(ErrorCodes.loginErrorStruct), delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").read()

        val flProc = new FailedLoginProcessor(failedLoginReader = failedLoginReader, citiesData = transactionData.cities, hotspotData= orderdDBData.wlanHotspot, errorCodes = errorCodes)

        val sessionDOutFile = "/Users/ondrejmachacek/tmp/hotspot/CPTM_TA_X_WLAN_SESSION_D_Daily_20200407.csv"

        new SessionDWriter(path = sessionDOutFile, data = sessionD.sessionD).writeData()

         */
  }

  def main(args: Array[String]): Unit = {

    args(0) match {
      case "exchangeRates" => processExchangeRates()
      case "stage" => doStage()
      case "core" => doProcessingCore()
      case _ => logger.error("Processing mode unspecified")
    }
  }
}
