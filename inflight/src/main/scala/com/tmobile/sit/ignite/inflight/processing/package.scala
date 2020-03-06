package com.tmobile.sit.ignite.inflight

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.max

package object processing {
  def getDefaultExchangeRates(exchangeRates: DataFrame) : DataFrame = {
    exchangeRates
      .groupBy("currency_code")
      .agg(max("valid_date").alias("valid_date"))
      .join(exchangeRates, Seq("currency_code", "valid_date"))
      .select("currency_code","conversion" )
      .withColumnRenamed("currency_code", "currency")
      .withColumnRenamed("conversion", "conversion_default")
  }

}
