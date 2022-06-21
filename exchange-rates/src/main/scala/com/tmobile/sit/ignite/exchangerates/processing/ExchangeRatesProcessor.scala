package com.tmobile.sit.ignite.exchangerates.processing

import java.sql.Date

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.readers.ExchangeRatesStageReader
import com.tmobile.sit.ignite.exchangerates.config.Settings
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Processor class orchestrating all the steps for exchange rates calculation
 * @param sparkSession
 * @param settings
 */

class ExchangeRatesProcessor(implicit sparkSession: SparkSession, settings: Settings) extends Logger {
  val MAX_DATE = Date.valueOf("4712-12-31")

  def process(): Unit = {
    logger.info(s"Reading exchange rate input file ${settings.inputConfig.exchange_rates_filename.get}")
    val exchangeRatesReader = new ExchangeRatesReader(settings.inputConfig.exchange_rates_filename.get)
    logger.info("Reading of the last exchange rates file")
    val oldExchangeFilesRaw =
      sparkSession
        .read
        .parquet(settings.stageConfig.exchange_rates_filename.get)
        .filter(col("date") < lit(settings.appConfig.input_date.get).cast(DateType))

    val oldExchangeFiles = oldExchangeFilesRaw
      .join(oldExchangeFilesRaw.select(max("date").alias("date")), Seq("date"),"inner" )
        .drop("date")

    logger.info("Initialising exchange rates processor")
    val exchangeRatesProcessor = new ExchangeRatesActualProcessor(exchangeRatesReader.read(), oldExchangeFiles, MAX_DATE)
    logger.info("Processing new exchange rates")
    val exchRatesFinal = exchangeRatesProcessor.runProcessing()
    logger.info(s"Writing new exchange rates file to ${settings.stageConfig.exchange_rates_filename.get}")
    exchRatesFinal
        .withColumn("date", lit(settings.appConfig.input_date.get).cast(DateType))
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .parquet(settings.stageConfig.exchange_rates_filename.get)

    //CSVWriter(data = exchRatesFinal, path = settings.stageConfig.exchange_rates_filename.get, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").writeData()
  }
}
