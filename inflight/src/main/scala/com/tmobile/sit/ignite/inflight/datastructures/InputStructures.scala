package com.tmobile.sit.ignite.inflight.datastructures

import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType}

/**
 * Type definitions for all the input files - remark - loadId and entryID will hopefully be removed in the future
 */

object InputStructures {

  val aircraftStruct = StructType(
    Seq(
      StructField("tailsign", StringType, true),
      StructField("manufacturer", StringType, true),
      StructField("ac_type", StringType, true),
      StructField("serial_number", StringType, true),
      StructField("year_of_manufacture", LongType, true),
      StructField("name", StringType, true),
      StructField("airline", StringType, true),
      StructField("icao_type", StringType, true),
      StructField("iata_type", StringType, true),
      StructField("gcs_equipped", StringType, true),
      StructField("xid", LongType, true),
      StructField("hotspot_id", StringType, true)
    )
  )
  val airlineStructure = StructType(Seq(
    StructField("airline_name", StringType, true),
    StructField("airline_icao", StringType, true),
    StructField("airline_iata", StringType, true),
    StructField("airline_logo_file", StringType, true)
  )
  )
  val airportStructure = StructType(Seq(
    StructField("airport_icao", StringType, true),
    StructField("airport_iata", StringType, true),
    StructField("airport_name", StringType, true),
    StructField("airport_city", StringType, true),
    StructField("airport_country", StringType, true),
    StructField("airport_latitude", DoubleType, true),
    StructField("airport_longitude", DoubleType, true),
    StructField("airport_coverage", StringType, true)
  )
  )
  val oooidStructure = StructType(Seq(
    StructField("wlif_sequence", LongType, true),
    StructField("wlif_method", StringType, true),
    StructField("wlif_flight_id", LongType, true),
    StructField("wlif_auid", StringType, true),
    StructField("wlif_xid_pac", LongType, true),
    StructField("wlif_airline_code", StringType, true),
    StructField("wlif_aircraft_code", StringType, true),
    StructField("wlif_flight_number", StringType, true),
    StructField("wlif_airport_code_origin", StringType, true),
    StructField("wlif_airport_code_destination", StringType, true),
    StructField("wlif_date_time_event", TimestampType, true),
    StructField("wlif_date_time_received", TimestampType, true)
  )
  )
  val realmStructure = StructType(Seq(
    StructField("realm_prefix", StringType, true),
    StructField("account_type", StringType, true)
  )
  )
  val radiusStructure = StructType(
    Seq(
      StructField("wlif_session_id", StringType, true),
      StructField("wlif_user", StringType, true),
      StructField("wlif_username", StringType, true),
      StructField("wlif_realm_code", StringType, true),
      StructField("wlif_account_type", StringType, true),
      StructField("wlif_prefix", StringType, true),
      StructField("wlan_hotspot_ident_code", StringType, true),
      StructField("wlif_xid_pac", LongType, true),
      StructField("wlif_aircraft_code", StringType, true),
      StructField("wlif_flight_id", LongType, true),
      StructField("wlif_airline_code", StringType, true),
      StructField("wlif_flight_number", StringType, true),
      StructField("wlif_airport_code_origin", StringType, true),
      StructField("wlif_airport_code_destination", StringType, true),
      StructField("wlif_session_start", TimestampType, true),
      StructField("wlif_session_stop", TimestampType, true),
      StructField("wlif_session_time", LongType, true),
      StructField("wlif_in_volume", DoubleType, true),
      StructField("wlif_out_volume", DoubleType, true),
      StructField("wlif_termination_cause", StringType, true)

    )
  )

  val flightLegStructure = StructType(
    Seq(
      StructField("wlif_flight_id", LongType, true),
      StructField("wlif_flightleg_status", StringType, true),
      StructField("wlif_airline_code", StringType, true),
      StructField("wlif_aircraft_code", StringType, true),
      StructField("wlif_flight_number", StringType, true),
      StructField("wlif_airport_code_origin", StringType, true),
      StructField("wlif_airport_code_destination", StringType, true),
      StructField("wlif_date_time_opened", TimestampType, true),
      StructField("wlif_method_opened", StringType, true),
      StructField("wlif_date_time_closed", TimestampType, true),
      StructField("wlif_method_closed", StringType, true),
      StructField("wlif_xid_pac", LongType, true),
      StructField("wlif_num_users", LongType, true),
      StructField("wlif_num_sessions", LongType, true),
      StructField("wlif_session_time", LongType, true),
      StructField("wlif_session_volume_out", DoubleType, true),
      StructField("wlif_session_volume_in", DoubleType, true),
      StructField("wlif_active_sessions", LongType, true)
    )
  )

  val orderdbStructure = StructType(Seq(
    StructField("ta_id", StringType, true),
    StructField("ta_request_date", DateType, true),
    StructField("ta_request_datetime", TimestampType, true),
    StructField("ta_request_hour", StringType, true),
    StructField("payid", StringType, true),
    StructField("error_code", StringType, true),
    StructField("email", StringType, true),
    StructField("amount", DoubleType, true),
    StructField("currency", StringType, true),
    StructField("result_code", StringType, true),
    StructField("cancellation", StringType, true),
    StructField("card_institute", StringType, true),
    StructField("vat", DoubleType, true),
    StructField("payment_method", StringType, true),
    StructField("voucher_type", StringType, true),
    StructField("hotspot_country_code", StringType, true),
    StructField("hotspot_provider_code", StringType, true),
    StructField("hotspot_venue_type_code", StringType, true),
    StructField("hotspot_venue_code", StringType, true),
    StructField("hotspot_city_code", StringType, true),
    StructField("hotspot_ident_code", StringType, true),
    StructField("hotspot_timezone", StringType, true),
    StructField("natco", StringType, true),
    StructField("username", StringType, true),
    StructField("wlan_realm_code", StringType, true),
    StructField("ma_name", StringType, true),
    StructField("voucher_duration", LongType, true),
    StructField("alternate_amount", DoubleType, true),
    StructField("alternate_currency", StringType, true),
    StructField("reduced_amount", DoubleType, true),
    StructField("campaign_name", StringType, true),
    StructField("entry_id", LongType, false),
    StructField("load_date", TimestampType, false)
  ))

  val mapVoucherStructure = StructType(
    Seq(
      StructField("wlan_ta_id", StringType, true),
      StructField("wlan_request_date", TimestampType, true),
      StructField("wlan_username", StringType, true),
      StructField("wlif_username", StringType, true),
      StructField("wlif_realm_code", StringType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  )
  val exchangeRatesStructure = StructType(
    Seq(
      StructField("currency_code", StringType, true),
      StructField("exchange_rate_code", StringType, true),
      StructField("exchange_rate_avg", DoubleType, true),
      StructField("exchange_rate_sell", DoubleType, true),
      StructField("exchange_rate_buy", DoubleType, true),
      StructField("faktv", LongType, true),
      StructField("faktn", LongType, true),
      StructField("period_from", TimestampType, true),
      StructField("period_to", TimestampType, true),
      StructField("valid_from", DateType, true),
      StructField("valid_to", DateType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  )



}
