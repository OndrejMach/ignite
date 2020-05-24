package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.ignite.common.data.CommonStructures

object OutputStructures {
  val EXCHANGE_RATES_OUTPUT_COLUMNS : Seq[String]= CommonStructures.exchangeRatesStructure.map(_.name)

  val SESSION_D_OUTPUT_COLUMNS = Seq("wlan_session_date",
    "wlan_hotspot_ident_code",
    "wlan_provider_code",
    "wlan_user_account_id",
    "wlan_user_provider_code",
    "terminate_cause_id",
    "login_type",
    "session_duration",
    "session_volume",
    "num_of_stop_tickets",
    "num_of_gen_stop_tickets",
    "num_subscriber")

  val VOUCHER_OUTPUT_COLUMNS = Seq("wlan_voucher_id","wlan_voucher_code",
    "wlan_voucher_desc","tmo_country_code",
    "duration","price","vat",
    "conversion","valid_from","valid_to")

  val CITIES_OUTPUT_COLUMNS = Seq("city_id", "city_code", "city_desc", "city_ldesc")

  val FAILED_TRANSACTIONS_COLUMNS = Seq("request_hour","country_code",
    "wlan_hotspot_id","city_id",
    "wlan_provider_code","wlan_venue_type_code",
    "wlan_venue_code","wlan_voucher_id",
    "wlan_voucher_type","wlan_card_institute_code",
    "wlan_payment_type_code","wlan_transac_type_id",
    "wlan_error_code","num_of_failed_transac",
    "currency","vat","num_flight_miles")

  val ORDERDB_H_COLUMNS = Seq("request_hour","country_code",
    "wlan_hotspot_id","city_id",
    "wlan_provider_code","wlan_venue_type_code",
    "wlan_venue_code","wlan_voucher_id",
    "wlan_voucher_type","wlan_card_institute_code",
    "wlan_payment_type_code","wlan_transac_type_id",
    "currency","vat","discount_rel",
    "campaign_name","num_of_transactions",
    "amount_d_incl_vat","amount_d_excl_vat",
    "amount_d_incl_vat_lc","amount_d_excl_vat_lc",
    "amount_c_incl_vat","amount_c_excl_vat",
    "amount_c_incl_vat_lc","amount_c_excl_vat_lc",
    "amount_incl_vat","amount_excl_vat",
    "amount_incl_vat_lc","amount_excl_vat_lc",
    "num_flight_miles")

  val SESSION_Q_COLUMNS = Seq("wlan_session_date","quarter_of_an_hour_id",
    "wlan_hotspot_ident_code","wlan_provider_code",
    "wlan_user_account_id","wlan_user_provider_code",
    "terminate_cause_id","login_type",
    "session_duration","session_volume",
    "num_of_session_start","num_of_session_stop",
    "num_of_session_active","num_subscriber")


  val FAILED_LOGINS_OUTPUT_COLUMNS = Seq(
    "login_datetime","login_hour","hotspot_id",
    "hotspot_ident_code","city_id",
    "city_name","hotspot_provider_code",
    "hotspot_venue_code","hotspot_venue_type_code",
    "hotspot_country_code","user_provider",
    "account_type_id","login_type",
    "login_error_code","num_of_failed_logins")
  }
