package com.tmobile.sit.ignite.inflight

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.max

import scala.util.Random

package object processing {
  def getDefaultExchangeRates(exchangeRates: DataFrame) : DataFrame = {
    //exchangeRates.printSchema()

    exchangeRates
      .groupBy("currency_code")
      .agg(max("valid_to").alias("valid_to"))
      .join(exchangeRates, Seq("currency_code", "valid_to"))
      .select("currency_code","conversion" )
      .withColumnRenamed("currency_code", "currency")
      .withColumnRenamed("conversion", "conversion_default")
  }

}
