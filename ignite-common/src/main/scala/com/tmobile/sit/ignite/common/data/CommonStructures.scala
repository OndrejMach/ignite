package com.tmobile.sit.ignite.common.data

import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType}

object CommonStructures {
  val exchangeRatesStructure = StructType(
    Seq(
      StructField("currency_code", StringType, true),
      StructField("exchange_rate_code", StringType, true),
      StructField("exchange_rate_avg", DoubleType, true),
      StructField("exchange_rate_sell", DoubleType, true),
      StructField("exchange_rate_buy", DoubleType, true),
      StructField("faktv", LongType, true),
      StructField("faktn", LongType, true),
      StructField("period_from", TimestampType, true),
      StructField("period_to", TimestampType, true),
      StructField("valid_from", DateType, true),
      StructField("valid_to", DateType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  )

}
