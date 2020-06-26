package com.tmobile.sit.ignite.rcse.processors.datastructures

object EventsStage {
  val stageColumns = Seq("date_id", "natco_code",
    "msisdn", "imsi",
    "rcse_event_type", "rcse_subscribed_status_id",
    "rcse_active_status_id", "rcse_tc_status_id",
    "tac_code", "rcse_version",
    "rcse_client_id", "rcse_terminal_id",
    "rcse_terminal_sw_id")

  val withLookups = Seq( "date_id", "natco_code",
    "msisdn", "imsi",
    "rcse_event_type", "rcse_subscribed_status_id",
    "rcse_active_status_id", "rcse_tc_status_id",
    "tac_code", "rcse_version",
    "rcse_client_id", "rcse_terminal_id",
    "rcse_terminal_sw_id", "terminal_id",
    "client_vendor", "client_version",
    "terminal_vendor", "terminal_model",
    "terminal_sw_version", "imei")

  val input = Seq("date_id", "msisdn", "imsi","rcse_event_type",
     "rcse_subscribed_status_id", "rcse_active_status_id",
      "rcse_tc_status_id", "imei",
      "rcse_version", "client_vendor",
      "client_version", "terminal_vendor",
      "terminal_model", "terminal_sw_version"
  )
}