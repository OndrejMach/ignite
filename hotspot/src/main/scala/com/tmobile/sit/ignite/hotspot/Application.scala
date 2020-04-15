package com.tmobile.sit.ignite.hotspot

import java.sql.Date

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.data.CommonStructures
import com.tmobile.sit.ignite.hotspot.processors.ExchangeRatesProcessor
import com.tmobile.sit.ignite.hotspot.readers.ExchangeRatesReader


object Application extends App {
  implicit val sparkSession = getSparkSession()
  val MAX_DATE = Date.valueOf("4712-12-31")
  val outputFile = "/Users/ondrejmachacek/tmp/hotspot/exchangeRates.csv"
  val inputFile = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/CUP_exchangerates_d_20200408_1.csv"

  val exchangeRatesReader = new ExchangeRatesReader(inputFile)
  val oldExchangeFilesReader = CSVReader(path = outputFile, schema = Some(CommonStructures.exchangeRatesStructure), header = false, delimiter = "|")
  val oldData = oldExchangeFilesReader.read()
  CSVWriter(data = oldExchangeFilesReader.read(), path = outputFile+".previous", delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()

  val exchangeRatesProcessor = new ExchangeRatesProcessor(exchangeRatesReader, oldExchangeFilesReader,MAX_DATE)

  val exchRatesFinal = exchangeRatesProcessor.runProcessing()

  CSVWriter(data = exchRatesFinal, path = outputFile, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()

}
