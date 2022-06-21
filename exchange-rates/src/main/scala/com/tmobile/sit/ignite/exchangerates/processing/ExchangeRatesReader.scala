package com.tmobile.sit.ignite.exchangerates.processing

import com.tmobile.sit.ignite.common.common.readers.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * helper reader for exchange rates
 * @param path
 */

class ExchangeRatesReader(path: String)(implicit sparkSession: SparkSession) extends Reader{
  override def read(): DataFrame = {
    logger.info(s"getting exchange rates from file ${path}")
    sparkSession
      .read
      .option("header", "true")
      .text(path)
  }
}
