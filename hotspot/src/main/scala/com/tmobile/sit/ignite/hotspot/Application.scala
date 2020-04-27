package com.tmobile.sit.ignite.hotspot

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.data.CommonStructures
import com.tmobile.sit.ignite.hotspot.config.{OrderDBConfig, Settings}
import com.tmobile.sit.ignite.hotspot.data.OrderDBInputData
import com.tmobile.sit.ignite.hotspot.processors.{CDRProcessor, ExchangeRatesProcessor, OrderDBProcessor}
import com.tmobile.sit.ignite.hotspot.readers.{ExchangeRatesReader, TextReader}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.tmobile.sit.ignite.hotspot.data.FUTURE


object Application extends App {
  implicit val sparkSession = getSparkSession()

  import sparkSession.implicits._

  val MAX_DATE = Date.valueOf("4712-12-31")

  val PROCESSING_DATE = Date.valueOf(LocalDate.now())

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
  //aggregSession_d
  //TBL_NAME=session_d
  //TBL_PREF=cptm_ta_x_wlan
  val aggreg = cdrData
    .withColumn("wlan_hotspot_ident_code", when($"hotspot_id".isNotNull, $"hotspot_id").otherwise(concat(lit("undefined_"), $"hotspot_owner_id")))
    .withColumn("stop_ticket", when($"terminate_cause_id".equalTo(lit(1001)), 1).otherwise(0))
    .select($"wlan_session_date",
      $"wlan_hotspot_ident_code",
      $"hotspot_owner_id".as("wlan_provider_code"),
      $"wlan_user_account_id",
      $"user_provider_id".as("wlan_user_provider_code"),
      $"terminate_cause_id",
      $"login_type",
      $"session_duration",
      $"session_volume",
      $"venue_type",
      $"venue",
      $"country_code",
      $"stop_ticket",
      $"english_city_name")
    .sort()
    .groupBy("wlan_session_date", "wlan_user_provider_code", "wlan_provider_code", "wlan_hotspot_ident_code", "wlan_user_account_id", "terminate_cause_id", "login_type")
    .agg(
      first("terminate_cause_id").alias("terminate_cause_id"),
      first("venue_type").alias("venue_type"),
      first("venue").alias("venue"),
      sum("session_duration").alias("session_duration"),
      sum("session_volume").alias("session_volume"),
      first("country_code").alias("country_code"),
      first("english_city_name").alias("english_city_name"),
      sum("stop_ticket").alias("num_of_gen_stop_tickets"),
      count("*").alias("num_of_stop_tickets")
    )
    .withColumn("num_subscriber", lit(1).cast(IntegerType))

  val todayDataHotspot = orderdDBData.wlanHotspot.filter($"valid_to" >= lit(PROCESSING_DATE))
  val oldDataHotspot = orderdDBData.wlanHotspot.filter(!($"valid_to" >= lit(PROCESSING_DATE)))

  val sessionDOut = todayDataHotspot
    .join(aggreg, Seq("wlan_hotspot_ident_code"), "outer")

  sessionDOut.printSchema()

  //aggreg.show(false)

}
