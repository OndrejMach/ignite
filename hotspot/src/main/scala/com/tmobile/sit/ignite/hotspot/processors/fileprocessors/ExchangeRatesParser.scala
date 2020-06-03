package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.data.ExchangeRates
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * parsing input file with new exchange rates
 * @param exchangeRatesRawData - raw data read from the input file
 * @param sparkSession
 */


class ExchangeRatesParser(exchangeRatesRawData: DataFrame)(implicit sparkSession: SparkSession) extends Logger{
  val getPeriodDates: (Timestamp, Timestamp) = {
    import sparkSession.implicits._
    logger.info("Retrieving header from data")
    val header =   exchangeRatesRawData
      .filter(col("value").startsWith("H|"))
      .as[String]
      .first()

    logger.info(s"parsing header ${header}")

    val periodFrom = Timestamp.valueOf(LocalDateTime.parse(header.split("\\|")(7), DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
    val periodTo = Timestamp.valueOf(LocalDateTime.parse(header.split("\\|")(8), DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
    logger.info(s"header parsed - periodFrom: ${periodFrom}, periodTo: ${periodTo}")
    (periodFrom,periodTo)
  }

  val getData : Dataset[ExchangeRates] = {
    import sparkSession.implicits._
    logger.info("Parsing exchange rates data")
    exchangeRatesRawData
      .filter((!col("value").startsWith("H|")) && (col("value").startsWith("L|")))
      .as[String]
      .map(ExchangeRates.fromString(_))
  }
}
