package com.tmobile.sit.ignite.rcse.structures

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}

object InitUsers {
  val initUsersSchema = StructType(
    Seq(
      StructField("date_id", DateType, true),
      StructField("natco_code", StringType, true),
      StructField("rcse_init_client_id", IntegerType, true),
      StructField("rcse_init_terminal_id", IntegerType, true),
      StructField("rcse_init_terminal_sw_id", IntegerType, true),
      StructField("rcse_reg_users_new", IntegerType, true),
      StructField("rcse_reg_users_all", IntegerType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)
    )
  )
  val stageColumns = Seq("date_id", "natco_code", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id", "rcse_reg_users_new", "rcse_reg_users_all")

  val workColumns = Seq("date_id", "natco_code", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_old_terminal_id", "rcse_init_terminal_sw_id", "rcse_reg_users_new", "rcse_reg_users_all")

}
