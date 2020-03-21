package com.tmobile.sit.ignite.inflight.datastructures

import java.sql.Timestamp

object StageTypes {

  case class Aircraft(
                       wlif_aircraft_code: String,
                       wlif_aircraft_desc: String,
                       wlif_manufacturer: String,
                       wlif_ac_type: String,
                       wlif_serial_number: Option[String],
                       wlif_year_of_manufacture: Long,
                       wlif_airline_code: String,
                       wlif_icao_type: String,
                       wlif_iata_type: String,
                       wlif_gcs_equipped: String,
                       wlif_xid_pac: Long,
                       wlan_hotspot_ident_code: String
                     )

  case class Airline(
                      wlif_airline_code: Option[String],
                      wlif_airline_desc: String,
                      wlif_airline_iata: Option[String],
                      wlif_airline_logo_file: Option[String]
                    )

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

  case class Realm(
                    wlif_realm_code: Option[String],
                    wlif_realm_desc: Option[String],
                    wlif_account_type: Option[String]
                  )

  case class Oooi(
                   wlif_sequence: Option[Long],
                   wlif_method: Option[String],
                   wlif_flight_id: Option[Long],
                   wlif_auid: Option[String],
                   wlif_xid_pac: Option[Long],
                   wlif_airline_code: Option[String],
                   wlif_aircraft_code: Option[String],
                   wlif_flight_number: Option[String],
                   wlif_airport_code_origin: Option[String],
                   wlif_airport_code_destination: Option[String],
                   wlif_date_time_event: Option[Timestamp],
                   wlif_date_time_received: Option[Timestamp],
                   entry_id: Option[Long],
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
                     wlif_xid_pac: Option[Long],
                     wlif_aircraft_code: Option[String],
                     wlif_flight_id: Option[Long],
                     wlif_airline_code: Option[String],
                     wlif_flight_number: Option[String],
                     wlif_airport_code_origin: Option[String],
                     wlif_airport_code_destination: Option[String],
                     wlif_session_start: Option[Timestamp],
                     wlif_session_stop: Option[Timestamp],
                     wlif_session_time: Option[Long],
                     wlif_in_volume: Option[Double],
                     wlif_out_volume: Option[Double],
                     wlif_termination_cause: Option[String],
                     entry_id: Long,
                     load_date: Timestamp
                   )

  case class FlightLeg(
                        wlif_flight_id:Option[Long],
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
                        wlif_xid_pac: Option[Long],
                        wlif_num_users: Option[Long],
                        wlif_num_sessions: Option[Long],
                        wlif_session_time: Option[Long],
                        wlif_session_volume_out: Option[Double],
                        wlif_session_volume_in: Option[Double],
                        wlif_active_sessions: Option[Long],
                        entry_id: Long,
                        load_date: Timestamp
                      )
}