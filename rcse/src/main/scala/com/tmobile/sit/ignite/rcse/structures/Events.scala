package com.tmobile.sit.ignite.rcse.structures

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object Events {
  val eventsSchema = StructType(
    Seq(
      StructField("date_id", TimestampType, true),
      StructField("msisdn", LongType, true),
      StructField("imsi", StringType, true),
      StructField("rcse_event_type", StringType, true),
      StructField("rcse_subscribed_status_id", IntegerType, true),
      StructField("rcse_active_status_id", IntegerType, true),
      StructField("rcse_tc_status_id", IntegerType, true),
      StructField("imei", StringType, true),
      StructField("rcse_version", StringType, true),
      StructField("client_vendor", StringType, true),
      StructField("client_version", StringType, true),
      StructField("terminal_vendor", StringType, true),
      StructField("terminal_model", StringType, true),
      StructField("terminal_sw_version", StringType, true)
    )
  )
}
