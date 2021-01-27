package com.tmobile.sit.ignite.rcse.structures

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}

object Conf {
  val confFileSchema = StructType(
    Seq(
      StructField("date_id", DateType, true),
      StructField("natco_code", StringType, true),
      StructField("msisdn", StringType, true),
      StructField("rcse_tc_status_id", IntegerType, true),
      StructField("rcse_init_client_id", IntegerType, true),
      StructField("rcse_init_terminal_id", IntegerType, true),
      StructField("rcse_init_terminal_sw_id", IntegerType, true),
      StructField("rcse_curr_client_id", IntegerType, true),
      StructField("rcse_curr_terminal_id", IntegerType, true),
      StructField("rcse_curr_terminal_sw_id", IntegerType, true),
      StructField("modification_date", DateType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)
    )
  )
}

