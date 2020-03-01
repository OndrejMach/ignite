package com.tmobile.sit.ignite.inflight.datastructures

import java.sql.Timestamp

object StageStructures {

  /*
wlif_aircraft_code               string
wlif_aircraft_desc               string       enc="utf8"
wlif_manufacturer                string       null=""
wlif_ac_type                     string       null=""
wlif_serial_number               string       null=""
wlif_year_of_manufacture         int          null=""
wlif_airline_code                string       null=""
wlif_icao_type                   string       null=""
wlif_iata_type                   string       null=""
wlif_gcs_equipped                string       null=""
wlif_xid_pac                     int
wlan_hotspot_ident_code          string       null=""
 */
  case class Aircraft(
                       wlif_aircraft_code: String,
                       wlif_aircraft_desc: String,
                       wlif_manufacturer: String,
                       wlif_ac_type: String,
                       wlif_serial_number: Option[String],
                       wlif_year_of_manufacture: Int,
                       wlif_airline_code: String,
                       wlif_icao_type: String,
                       wlif_iata_type: String,
                       wlif_gcs_equipped: String,
                       wlif_xid_pac: Int,
                       wlan_hotspot_ident_code: String
                     )

  /*
  wlif_airline_code                string
wlif_airline_desc                string
wlif_airline_iata                string       null=""
wlif_airline_logo_file           string       null=""
   */
  case class Airline(
                      wlif_airline_code: Option[String],
                      wlif_airline_desc: String,
                      wlif_airline_iata: Option[String],
                      wlif_airline_logo_file: Option[String]
                    )

  /*
  wlif_airport_code               string
wlif_airport_desc               string
wlif_iata                       string        null=""
wlif_city                       string        null=""
wlif_country                    string        null=""
wlif_latitude                   decimal(18,8) null=""
wlif_longitude                  decimal(18,8) null=""
wlif_coverage                   string        null=""
   */
  case class Airport(
                      wlif_airport_code: Option[String],
                      wlif_airport_desc: String,
                      wlif_iata: Option[String],
                      wlif_city: Option[String],
                      wlif_country: Option[String],
                      wlif_latitude: Option[Double],
                      wlif_longitude: Option[Double],
                      wlif_coverage: Option[String]
                    )

  /*
  wlif_realm_code                  string
  wlif_realm_desc                  string
  wlif_account_type                string       null=""
   */
  case class Realm(
                    wlif_realm_code: Option[String],
                    wlif_realm_desc: Option[String],
                    wlif_account_type: Option[String]
                  )

  /*
  wlif_sequence                    int
wlif_method                      string       null=""
wlif_flight_id                   int          null=""
wlif_auid                        string       null=""
wlif_xid_pac                     int
wlif_airline_code                string       null=""
wlif_aircraft_code               string       null=""
wlif_flight_number               string       null=""
wlif_airport_code_origin         string       null=""
wlif_airport_code_destination    string       null=""
wlif_date_time_event             timestamp    null=""
wlif_date_time_received          timestamp    null=""
entry_id                         int
load_date                        timestamp
   */
  case class Oooi(
                   wlif_sequence: Option[Int],
                   wlif_method: Option[String],
                   wlif_flight_id: Option[Int],
                   wlif_auid: Option[String],
                   wlif_xid_pac: Option[Int],
                   wlif_airline_code: Option[String],
                   wlif_aircraft_code: Option[String],
                   wlif_flight_number: Option[String],
                   wlif_airport_code_origin: Option[String],
                   wlif_airport_code_destination: Option[String],
                   wlif_date_time_event: Option[Timestamp],
                   wlif_date_time_received: Option[Timestamp],
                   entry_id: Option[Int],
                   load_date: Option[Timestamp]
                 )

  case class Radius(
                     wlif_session_id: Option[String],
                     wlif_user: Option[String],
                     wlif_username: Option[String],
                     wlif_realm_code: Option[String],
                     wlif_account_type: Option[String],
                     wlif_prefix: Option[String],
                     wlan_hotspot_ident_code: Option[String],
                     wlif_xid_pac: Option[Int],
                     wlif_aircraft_code: Option[String],
                     wlif_flight_id: Option[Int],
                     wlif_airline_code: Option[String],
                     wlif_flight_number: Option[String],
                     wlif_airport_code_origin: Option[String],
                     wlif_airport_code_destination: Option[String],
                     wlif_session_start: Option[Timestamp],
                     wlif_session_stop: Option[Timestamp],
                     wlif_session_time: Option[Int],
                     wlif_in_volume: Option[Double],
                     wlif_out_volume: Option[Double],
                     wlif_termination_cause: Option[String],
                     entry_id: Int,
                     load_date: Timestamp
                   )

  case class FlightLeg(
                        wlif_flight_id:Option[Int],
                        wlif_flightleg_status: Option[String],
                        wlif_airline_code: Option[String],
                        wlif_aircraft_code: Option[String],
                        wlif_flight_number: Option[String],
                        wlif_airport_code_origin: Option[String],
                        wlif_airport_code_destination: Option[String],
                        wlif_date_time_opened: Option[Timestamp],
                        wlif_method_opened: Option[String],
                        wlif_date_time_closed: Option[Timestamp],
                        wlif_method_closed: Option[String],
                        wlif_xid_pac: Option[Int],
                        wlif_num_users: Option[Int],
                        wlif_num_sessions: Option[Int],
                        wlif_session_time: Option[Int],
                        wlif_session_volume_out: Option[Double],
                        wlif_session_volume_in: Option[Double],
                        wlif_active_sessions: Option[Int],
                        entry_id: Int,
                        load_date: Timestamp
                      )

}