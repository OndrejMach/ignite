package com.tmobile.sit.ignite.hotspot.data

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
 * definition of country code input data
 */

object StagedataStructs {
  val country_code_structure = StructType(
    Seq(
      StructField("country_code", StringType, true),
      StructField("tmo_country_code", StringType, true),
      StructField("country_desc", StringType, true),
      StructField("national_dialing_code", StringType, true),
      StructField("currency_code", StringType, true),
      StructField("valid_from", TimestampType, true),
      StructField("valid_to", TimestampType, true)
    )
  )
}
