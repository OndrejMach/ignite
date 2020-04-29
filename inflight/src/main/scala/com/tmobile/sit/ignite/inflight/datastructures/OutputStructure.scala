package com.tmobile.sit.ignite.inflight.datastructures

/**
 * here you can find columns which will be represented in the outputs. Keep in mind that column names are in CAPITAL letters there.
 * The interim structures are used for staging and internal application state keep. In case you'd like to add/remove column from the output here you can do it.
 * The calculation always contains the superset of the available columns (if possible of course ;))
 */

object OutputStructure {
  val radiusCreditDailyColumns = Seq("wlif_session_stop","wlif_aircraft_code",
    "wlif_flight_id","wlif_airline_code",
    "wlif_flight_number","wlif_airport_code_origin",
    "wlif_airport_code_destination","wlif_username",
    "wlif_realm_code","wlan_hotspot_ident_code",
    "payid","amount_incl_vat","amount_excl_vat",
    "card_institute","payment_method",
    "voucher_type","voucher_duration",
    "wlif_num_sessions","wlif_session_volume","wlif_session_time")

  val radiusCreditStageColumns = Seq()

  val voucherRadiusDailyColumns = Seq("wlif_date_time_opened","wlif_date_time_closed",
    "wlif_flight_id","wlif_flight_number",
    "wlif_airport_code_origin","wlif_airport_code_destination",
    "wlif_realm_code","wlif_airline_code",
    "wlif_account_type","wlan_ta_id",
    "wlan_pay_id","wlan_card_institute",
    "wlan_payment_method","wlan_voucher_type",
    "wlan_voucher_duration","wlan_hotspot_ident_code",
    "amount_incl_vat","amount_excl_vat",
    "wlif_num_sessions","wlif_session_volume",
    "wlif_session_time","campaign_name")

  val voucherRadiusFullColumns = Seq("wlif_date_time_opened","wlif_date_time_closed",
    "wlif_flight_id","wlif_flight_number",
    "wlif_realm_code","wlif_airline_code",
    "wlif_account_type","wlan_hotspot_ident_code",
    "non_voucher_users","non_voucher_sessions",
    "voucher_users","voucher_sessions",
    "flight_users","flight_sessions")

  val flightLeg = Seq("wlif_flight_id", "wlif_flightleg_status",
    "wlif_airline_code", "wlif_aircraft_code",
    "wlif_flight_number", "wlif_airport_code_origin",
      "wlif_airport_code_destination",
    "wlif_date_time_opened", "wlif_num_users",
    "wlif_num_sessions", "wlif_session_time",
    "wlif_session_volume_out", "wlif_session_volume_in")

  val airport = Seq("wlif_airport_code")

  val aircraft = Seq("wlif_aircraft_code")

  val airline = Seq("wlif_airline_code")

  val oooi = Seq("wlif_method", "wlif_flight_id",
    "wlif_xid_pac", "wlif_airline_code",
    "wlif_aircraft_code", "wlif_flight_number",
    "wlif_airport_code_origin", "wlif_airport_code_destination",
    "wlif_date_time_event", "wlif_date_time_received")

  val radius = Seq("wlif_session_id", "wlif_user",
    "wlif_account_type", "wlif_aircraft_code",
    "wlif_flight_id", "wlif_airline_code",
    "wlif_flight_number", "wlif_airport_code_origin",
    "wlif_airport_code_destination", "wlif_session_stop",
    "wlif_session_time", "wlif_in_volume", "wlif_out_volume")

  val dailyReportSessionInterim = Seq("wlif_airline_code",
    "wlif_session_stop",
    "wlif_aircraft_code",
    "wlif_flight_number",
    "wlif_flight_id",
    "voucher_duration",
    "wlif_airport_code_origin",
    "wlif_airport_code_destination",
    "number_of_users",
    "average_session_volume",
    "max_volume_on_flight",
    "wlif_num_sessions")

  val dailReportSessionOutput = Seq("Airline","Session Stop Date",
    "Tail Sign","Flight Number",
    "Flight ID","Voucher Duration (hours)",
    "Airport Origin","Airport Destination",
    "Number of Users","Average Session Volume (MB)",
    "Maximum Volume (MB) on a Flight (ID) Consumed by a Single User","Number of Sessions")

  val dailyReportVoucherInterim = Seq("wlif_airline_code",
    "wlif_date_time_closed",
    "wlif_date_time_opened",
    "wlif_flight_number",
    "wlif_flight_id",
    "wlif_airport_code_origin",
    "wlif_airport_code_destination",
    "wlan_hotspot_ident_code",
    "wlan_card_institute",
    "wlan_payment_method",
    "wlan_voucher_duration",
    "campaign_name",
    "wlif_session_volume",
    "wlif_session_time")

  val dailyReportVoucherOutput = Seq(
    "Airline","Flight Date Closed","Flight Date Opened","Flight Number",
    "Flight ID","Airport Origin",
    "Airport Destination","HotSpot Ident Code",
    "Card Institute","Payment Method",
    "Voucher Duration (hours)","Campaign Name",
    "Data Usage in MB","Session Time (HH:MM:SS)"
  )
  
  val TTableOutputs = Seq("WLIF_DATE_TIME_OPENED",
    "WLIF_DATE_TIME_CLOSED",
    "WLIF_FLIGHT_ID",
    "WLIF_FLIGHT_NUMBER",
    "WLIF_REALM_CODE",
    "WLIF_AIRLINE_CODE",
    "WLIF_ACCOUNT_TYPE",
    "WLAN_HOTSPOT_IDENT_CODE",
    "NON_VOUCHER_USERS",
    "NON_VOUCHER_SESSIONS",
    "VOUCHER_USERS",
    "VOUCHER_SESSIONS",
    "FLIGHT_USERS",
    "FLIGHT_SESSIONS")

}
