package com.tmobile.sit.ignite.hotspot.readers

import com.tmobile.sit.common.readers.Reader
import com.tmobile.sit.ignite.hotspot.Application.sparkSession
import org.apache.spark.sql.DataFrame

/**
 * helper reader for exchange rates
 * @param path
 */

class ExchangeRatesReader(path: String) extends Reader{
  override def read(): DataFrame = {
    logger.info(s"getting exchange rates from file ${path}")
    sparkSession
      .read
      .option("header", "true")
      .text(path)
  }
}
