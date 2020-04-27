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

  //Seq("currency_code", "exchange_rate_code", "exchange_rate_avg", "exchange_rate_sell", "exchange_rate_buy", "faktv","faktn","period_from", "period_to", "valid_from","valid_to", "entry_id", "load_date")
}
