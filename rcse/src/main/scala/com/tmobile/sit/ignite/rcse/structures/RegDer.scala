package com.tmobile.sit.ignite.rcse.structures

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}

object RegDer {
  val regDerSchema = StructType(
    Seq(
      StructField("date_id", DateType, true),
      StructField("natco_code", StringType, true),
      StructField("msisdn", StringType, true),
      StructField("imsi", StringType, true),
      StructField("rcse_event_type", StringType, true),
      StructField("rcse_subscribed_status_id", IntegerType, true),
      StructField("rcse_active_status_id", IntegerType, true),
      StructField("rcse_tc_status_id", IntegerType, true),
      StructField("tac_code", StringType, true),
      StructField("rcse_version", StringType, true),
      StructField("rcse_client_id", IntegerType, true),
      StructField("rcse_terminal_id", IntegerType, true),
      StructField("rcse_terminal_sw_id", IntegerType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)
    )
  )
}
