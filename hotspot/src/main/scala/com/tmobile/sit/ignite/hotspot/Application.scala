package com.tmobile.sit.ignite.hotspot

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.hotspot.processors.ExchangeRatesProcessor
import com.tmobile.sit.ignite.hotspot.readers.ExchangeRatesReader


object Application extends App {
  implicit val sparkSession = getSparkSession()
  val RUN_DATE = Date.valueOf(LocalDate.now())
  val outputFile = "/Users/ondrejmachacek/tmp/hotspot/exchangeRates.csv"
  val inputFile = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/CUP_exchangerates_d_20200408_1.csv"

  val exchangeRatesReader = new ExchangeRatesReader(inputFile)

  val exchangeRatesProcessor = new ExchangeRatesProcessor(exchangeRatesReader, RUN_DATE)

  val exchRatesFinal = exchangeRatesProcessor.runProcessing()

  CSVWriter(data = exchRatesFinal, path = outputFile, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()
}
