package com.tmobile.sit.ignite.hotspot

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.data.CommonStructures
import com.tmobile.sit.ignite.hotspot.config.{OrderDBConfig, Settings}
import com.tmobile.sit.ignite.hotspot.data.{FUTURE, OrderDBInputData, OrderDBStructures}
import com.tmobile.sit.ignite.hotspot.processors.{CDRProcessor, ExchangeRatesProcessor, FailedTransactionsProcessor, OrderDBProcessor, SessionDProcessor}
import com.tmobile.sit.ignite.hotspot.readers.{ExchangeRatesReader, TextReader}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}


object Application extends App {
  implicit val sparkSession = getSparkSession()

  import sparkSession.implicits._

  val MAX_DATE = Date.valueOf("4712-12-31")

  val PROCESSING_DATE = Date.valueOf(LocalDate.now())

  val WLAN_HOTSPOT_ODATE = Date.valueOf(LocalDate.of(2020, 4, 8))

  val outputFile = "/Users/ondrejmachacek/tmp/hotspot/exchangeRates.csv"
  val inputFile = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/CUP_exchangerates_d_20200408_1.csv"

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
  val FILE_DATE = Date.valueOf(LocalDate.now())


  val inputFileCDR = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.CDR.DAY.20200408*.csv"
  val reader = new TextReader(inputFileCDR)

  val processor = new CDRProcessor(reader, FILE_DATE)

  val cdrData = processor.processData()

  val sessionD = new SessionDProcessor(cdrData = cdrData, orderdDBData = orderdDBData, PROCESSING_DATE).processData()

  val orderDBplus1Data = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_f_wlan_orderdb.20200409.csv", schema = Some(OrderDBStructures.orderDBStruct),header = false, delimiter = "|" ).read()


  new FailedTransactionsProcessor(orderDBData = orderdDBData.orderDb, orderDBPLus1 = orderDBplus1Data, wlanHostspot = orderdDBData.wlanHotspot, WLAN_HOTSPOT_ODATE).processData()

  //when()

  //sessionDOut.printSchema()

  //aggreg.show(false)

}
