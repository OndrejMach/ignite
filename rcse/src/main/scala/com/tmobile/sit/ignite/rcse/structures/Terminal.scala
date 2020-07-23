package com.tmobile.sit.ignite.rcse.structures

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}

object Terminal {
  val terminalSchema = StructType(
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

  val terminal_d_struct = StructType(
    Seq(
      StructField("rcse_terminal_id", IntegerType, true),
      StructField("tac_code", StringType, true),
      StructField("terminal_id", IntegerType, true),
      StructField("rcse_terminal_vendor_sdesc", StringType, true),
      StructField("rcse_terminal_vendor_ldesc", StringType, true),
      StructField("rcse_terminal_model_sdesc", StringType, true),
      StructField("rcse_terminal_model_ldesc", StringType, true),
      StructField("modification_date", TimestampType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)
    )
  )
  val tac_struct = StructType(
    Seq(
      StructField("terminal_id", IntegerType, true),
      StructField("tac_code", StringType, true),
      StructField("id", IntegerType, true),
      StructField("manufacturer", StringType, true),
      StructField("model", StringType, true),
      StructField("model_alias", StringType, true),
      StructField("csso_alias", StringType, true),
      StructField("status", StringType, true),
      StructField("international_material_number", StringType, true),
      StructField("launch_date", DateType, true),
      StructField("gsm_bandwidth", StringType, true),
      StructField("gprs_capable", StringType, true),
      StructField("edge_capable", StringType, true),
      StructField("umts_capable", StringType, true),
      StructField("wlan_capable", StringType, true),
      StructField("form_factor", StringType, true),
      StructField("handset_tier", StringType, true),
      StructField("wap_type", StringType, true),
      StructField("wap_push_capable", StringType, true),
      StructField("colour_depth", StringType, true),
      StructField("mms_capable", StringType, true),
      StructField("camera_type", StringType, true),
      StructField("camera_resolution", StringType, true),
      StructField("video_messaging_capable", StringType, true),
      StructField("ringtone_type", StringType, true),
      StructField("java_capable", StringType, true),
      StructField("email_client", StringType, true),
      StructField("email_push_capable", StringType, true),
      StructField("operating_system", StringType, true),
      StructField("golden_gate_user_interface", StringType, true),
      StructField("tzones_hard_key", StringType, true),
      StructField("bluetooth_capable", StringType, true),
      StructField("tm3_capable", StringType, true),
      StructField("terminal_full_name", StringType, true),
      StructField("video_record", StringType, true),
      StructField("valid_from", DateType, true),
      StructField("valid_to", DateType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)

    )
  )

  val terminalDoutputCols = Seq("rcse_terminal_id", "tac_code", "terminal_id",
    "rcse_terminal_vendor_sdesc", "rcse_terminal_vendor_ldesc",
    "rcse_terminal_model_sdesc", "rcse_terminal_model_ldesc",
    "modification_date")
}
