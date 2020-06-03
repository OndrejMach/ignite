package com.tmobile.sit.ignite.hotspot.data

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

/**
 * Case classes for error code file read during processing.
 */

object ErrorCodes {
  val loginErrorStruct = StructType (
    Seq(
      StructField("error_id", StringType,true),
      StructField("error_desc", StringType,true),
      StructField("valid_from", TimestampType,true),
      StructField("valid_to", TimestampType,true)
    )
  )
  val error_codes_struct = StructType (
    Seq(
      StructField("error_code", StringType,true),
      StructField("error_message", StringType,true),
      StructField("error_desc", StringType,true),
      StructField("valid_from", TimestampType,true),
      StructField("valid_to", TimestampType,true)
    )
  )
}
