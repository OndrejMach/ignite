package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Date

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.data.CommonStructures
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.processors.fileprocessors.ExchangeRatesActualProcessor
import com.tmobile.sit.ignite.hotspot.readers.ExchangeRatesReader
import org.apache.spark.sql.SparkSession

/**
 * Processor class orchestrating all the steps for exchange rates calculation
 * @param sparkSession
 * @param settings
 */

class ExchangeRatesProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
  val MAX_DATE = Date.valueOf("4712-12-31")

  override def process(): Unit = {
    logger.info(s"Reading exchange rate input file ${settings.inputConfig.exchange_rates_filename.get}")
    val exchangeRatesReader = new ExchangeRatesReader(settings.inputConfig.exchange_rates_filename.get)
    logger.info("Reading of the last exchange rates file")
    val oldExchangeFilesReader = CSVReader(path = settings.stageConfig.exchange_rates_filename.get, schema = Some(CommonStructures.exchangeRatesStructure), header = false, delimiter = "|")
    logger.info("Backup of the last valid exchange rates file")
    CSVWriter(data = oldExchangeFilesReader.read(), path = settings.stageConfig.exchange_rates_filename.get + ".previous", delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()
    logger.info("Initialising exchange rates processor")
    val exchangeRatesProcessor = new ExchangeRatesActualProcessor(exchangeRatesReader, oldExchangeFilesReader, MAX_DATE)
    logger.info("Processing new exchange rates")
    val exchRatesFinal = exchangeRatesProcessor.runProcessing()
    logger.info(s"Writing new exchange rates file to ${settings.stageConfig.exchange_rates_filename.get}")
    CSVWriter(data = exchRatesFinal, path = settings.stageConfig.exchange_rates_filename.get, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()
  }
}
