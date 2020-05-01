package com.tmobile.sit.ignite.hotspot.data

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, TimestampType}

object InterimDataStructures {
  val CITY_STRUCT = StructType {
    Seq(
      StructField("city_id", LongType, true),
      StructField("city_code", StringType, true),
      StructField("city_desc", StringType, true),
      StructField("city_ldesc", StringType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  }

  val VOUCHER_STRUCT = StructType {
    Seq(
      StructField("wlan_voucher_id", LongType, true),
      StructField("wlan_voucher_code", StringType, true),
      StructField("wlan_voucher_desc", StringType, true),
      StructField("tmo_country_code", StringType, true),
      StructField("duration", StringType, true),
      StructField("price", DoubleType, true),
      StructField("vat", DoubleType, true),
      StructField("conversion", DoubleType, true),
      StructField("valid_from", TimestampType, true),
      StructField("valid_to", TimestampType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  }

}
