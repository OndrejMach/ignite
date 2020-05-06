package com.tmobile.sit.ignite.hotspot.data

object StageStructures {
  val ERROR_CODES = Seq("error_code", "error_message", "error_desc", "valid_from", "valid_to")
  val MAP_VOUCHER = Seq("wlan_ta_id", "wlan_request_date", "wlan_username", "wlif_username", "wlif_realm_code", "year", "month", "day")
  val ORDER_DB = Seq("ta_id","ta_request_date",
    "ta_request_datetime","ta_request_hour",
    "paytid","error_code",
    "email","amount",
    "currency","result_code",
    "cancellation","card_institute",
    "vat","payment_method",
    "voucher_type","hotspot_country_code",
    "hotspot_provider_code","hotspot_venue_type_code",
    "hotspot_venue_code","hotspot_city_code",
    "hotspot_ident_code","hotspot_timezone",
    "natco","username","wlan_realm_code",
    "ma_name","voucher_duration",
    "alternate_amount","alternate_currency",
    "reduced_amount","campaign_name",
    "year", "month", "day"
  )
  val WLAN_HOTSPOT = Seq(
   "wlan_hotspot_id","wlan_hotspot_ident_code",
    "wlan_hotspot_desc","wlan_hotspot_timezone",
    "wlan_ip_range_start_dec","wlan_ip_range_end_dec",
    "wlan_ip_range_start","wlan_ip_range_end",
    "wlan_hotspot_status","wlan_venue_type_code",
    "wlan_venue_code","wlan_provider_code",
    "wlan_hotspot_area_code","ssid",
    "country_code","city_code",
    "postcode","street",
    "house_no","access_control",
    "long_deg","long_min",
    "long_sec","lat_deg",
    "lat_min","lat_sec",
    "open_monday","open_tuesday",
    "open_wednesday","open_thursday",
    "open_friday","open_saturday",
    "open_sunday","open_comment",
    "parking_available","loc_description",
    "loc_spec_offer","loc_url",
    "loc_cont_tel","loc_cont_fax",
    "loc_cont_email","ap_amount",
    "coord_system","commercial_status",
    "parent_ident_code","bandwidth",
    "valid_from","valid_to"
  )
  
}
