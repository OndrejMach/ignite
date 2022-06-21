package com.tmobile.sit.ignite.common.readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.tmobile.sit.ignite.common.common.readers.Reader
import org.apache.spark.sql.functions.{lit, max, col, current_date}


class ExchangeRatesStageReader(path: String)(implicit sparkSession: SparkSession) extends Reader{
  override def read(): DataFrame = {
    val data = sparkSession
      .read
      .parquet(path)

    val maxDate =
      data
        //.filter(col("date") < current_date)
        .select(max("date")).collect()(0)(0)

    val ret = data
      .filter(col("date") === lit(maxDate))
      //.drop("date")

    ret.drop("date")
  }
}

object ExchangeRatesStageReader {
  def apply(path: String)(implicit sparkSession: SparkSession): ExchangeRatesStageReader = new ExchangeRatesStageReader(path)(sparkSession)
}
