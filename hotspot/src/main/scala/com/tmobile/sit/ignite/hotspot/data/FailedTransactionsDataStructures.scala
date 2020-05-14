package com.tmobile.sit.ignite.hotspot.data

object FailedTransactionsDataStructures {
  val KEY_COLUMNS_VOUCHER = Seq("natco", "voucher_type", "amount", "voucher_duration")
  val COLUMNS_VOUCHER = Seq("wlan_voucher_code", "voucher_type", "natco", "duration", "amount", "vat", "conversion", "valid_from", "valid_to")
  val JOIN_COLUMNS_VOUCHER = Seq("natco", "voucher_type", "amount", "duration")
  val KEY_AGG_ORDERDB_H = Seq("ta_request_hour",
    "wlan_hotspot_id", "country_code",
    "city_code", "wlan_provider_code",
    "wlan_venue_type_code", "wlan_venue_code",
    "wlan_voucher_id", "wlan_card_institute_code",
    "currency", "vat",
    "wlan_payment_type_code", "voucher_type",
    "wlan_transac_type_id", "campaign_name",
    "discount_rel"
  )

  val KEY_AGG_FAILED_TRANSAC = Seq("ta_request_hour",
    "wlan_hotspot_id", "country_code",
    "city_code", "wlan_provider_code",
    "wlan_venue_type_code", "wlan_venue_code",
    "wlan_voucher_id", "wlan_card_institute_code",
    "currency", "vat",
    "wlan_payment_type_code", "voucher_type",
    "wlan_transac_type_id", "wlan_error_code"
  )
}
