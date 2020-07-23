package com.tmobile.sit.ignite.rcse.structures

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object CommonStructures {
  val clientSchema = StructType(
    Seq(
      StructField("rcse_client_id", IntegerType, true),
      StructField("rcse_client_vendor_sdesc", StringType, true),
      StructField("rcse_client_vendor_ldesc", StringType, true),
      StructField("rcse_client_version_sdesc", StringType, true),
      StructField("rcse_client_version_ldesc", StringType, true),
      StructField("modification_date", TimestampType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)
    )
  )

  val terminalSWSchema = StructType(
    Seq(
      StructField("rcse_terminal_sw_id", IntegerType, true),
      StructField("rcse_terminal_sw_desc", StringType, true),
      StructField("modification_date", TimestampType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)

    )
  )

  val des3Schema = StructType(
    Seq(
      StructField("des", StringType, false),
      StructField("number", StringType, false)
    )
  )
}
