package com.tmobile.sit.ignite.hotspot

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.data.CommonStructures
import com.tmobile.sit.ignite.common.processing.NormalisedExchangeRates
import com.tmobile.sit.ignite.hotspot.config.{OrderDBConfig, Settings}
import com.tmobile.sit.ignite.hotspot.data.{ErrorCodes, FUTURE, InterimDataStructures, OrderDBInputData, OrderDBStructures}
import com.tmobile.sit.ignite.hotspot.processors.{CDRProcessor, ExchangeRatesProcessor, FailedLoginProcessor, FailedTransactionsProcessor, OrderDBProcessor, SessionDProcessor, SessionsQProcessor}
import com.tmobile.sit.ignite.hotspot.readers.{ExchangeRatesReader, TextReader}
import com.tmobile.sit.ignite.common.data.CommonTypes
import com.tmobile.sit.ignite.hotspot.data.FailedLoginsStructure.FailedLogin


object Application extends App {


  val MIN_REQUEST_DATE = Timestamp.valueOf("2017-01-01 00:00:00")
  val MAX_DATE = Date.valueOf("4712-12-31")

  val PROCESSING_DATE = Date.valueOf(LocalDate.now())

  val WLAN_HOTSPOT_ODATE = Date.valueOf(LocalDate.of(2020, 4, 8))

  val outputFile = "/Users/ondrejmachacek/tmp/hotspot/exchangeRates.csv"
  val inputFile = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/CUP_exchangerates_d_20200408_1.csv"
  val FILE_DATE = Date.valueOf(LocalDate.now())

  implicit val sparkSession = getSparkSession()
  implicit val processingDate = WLAN_HOTSPOT_ODATE

  import sparkSession.implicits._




  val exchangeRatesReader = new ExchangeRatesReader(inputFile)
  val oldExchangeFilesReader = CSVReader(path = outputFile, schema = Some(CommonStructures.exchangeRatesStructure), header = false, delimiter = "|")
  val oldData = oldExchangeFilesReader.read()
  CSVWriter(data = oldExchangeFilesReader.read(), path = outputFile + ".previous", delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()

  val exchangeRatesProcessor = new ExchangeRatesProcessor(exchangeRatesReader, oldExchangeFilesReader, MAX_DATE)

  val exchRatesFinal = exchangeRatesProcessor.runProcessing()

  CSVWriter(data = exchRatesFinal, path = outputFile, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()

  //OrderDB
  val settings: Settings = Settings(
    OrderDBConfig(wlanHotspotFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/cptm_ta_d_wlan_hotspot.csv"),
      orderDBFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.MPS.DAY.20200408*.csv"),
      errorCodesFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_error_code.csv")
    ))


  val orderDBProcessor = new OrderDBProcessor(orderDBInputData = OrderDBInputData(settings.orderDBFiles), maxDate = FUTURE)

  val orderdDBData = orderDBProcessor.processData()
  //CDR


  val inputFileCDR = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.CDR.DAY.20200408*.csv"
  val reader = new TextReader(inputFileCDR)

  val processor = new CDRProcessor(reader, FILE_DATE)

  val cdrData = processor.processData()

  val sessionD = new SessionDProcessor(cdrData = cdrData, orderdDBData = orderdDBData, PROCESSING_DATE).processData()

  //FailedTransactions
  val orderDBplus1Data = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_f_wlan_orderdb.20200409.csv", schema = Some(OrderDBStructures.orderDBStruct),header = false, delimiter = "|" ).read()


  val cityData = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/common/cptm_ta_d_city.csv", header = false, schema = Some(InterimDataStructures.CITY_STRUCT), delimiter = "|").read()
  val voucherData = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_voucher.csv", header = false, schema = Some(InterimDataStructures.VOUCHER_STRUCT), delimiter = "|").read()


  val transactionData = new FailedTransactionsProcessor(orderDBData = orderdDBData.orderDb,
    wlanHotspot = orderdDBData.wlanHotspot,
    orderDBDataPLus1 = orderDBplus1Data,
    oldCitiesData = cityData,
    oldVoucherData = voucherData,
    normalisedExchangeRates= new NormalisedExchangeRates(exchRatesFinal.as[CommonTypes.ExchangeRates],MIN_REQUEST_DATE)).processData()

  //cdrData.show(false)

  val emptyDF = sparkSession.emptyDataFrame

  val res = new SessionsQProcessor(cdrData, cdrData,cdrData,Timestamp.valueOf(LocalDateTime.of(2020,4, 6, 0, 0, 0, 0)) ).getData

  //res.show(false)

  val failedLoginReader = new TextReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.FAILEDLOGINS.DAY.20200411020101.csv")

  val errorCodes = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_login_error.csv", header = false, schema = Some(ErrorCodes.loginErrorStruct), delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").read()

  val flProc = new FailedLoginProcessor(failedLoginReader = failedLoginReader, citiesData = transactionData.cities, hotspotData= orderdDBData.wlanHotspot, errorCodes = errorCodes)

  //flProc.getData.show(false)

  //when()

  //sessionDOut.printSchema()

  //aggreg.show(false)

}
