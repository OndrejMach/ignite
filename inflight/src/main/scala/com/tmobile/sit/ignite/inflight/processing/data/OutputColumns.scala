package com.tmobile.sit.ignite.inflight.processing.data

object OutputColumns {
  val flightLeg = Seq("wlif_flight_id", "wlif_flightleg_status", "wlif_airline_code", "wlif_aircraft_code", "wlif_flight_number", "wlif_airport_code_origin", "wlif_airport_code_destination", "wlif_date_time_opened", "wlif_num_users", "wlif_num_sessions", "wlif_session_time", "wlif_session_volume_out", "wlif_session_volume_in")
  val airport = Seq("wlif_airport_code")
  val aircraft = Seq("wlif_aircraft_code")
  val airline = Seq("wlif_airline_code")
  val oooi = Seq("wlif_method", "wlif_flight_id", "wlif_xid_pac", "wlif_airline_code", "wlif_aircraft_code", "wlif_flight_number", "wlif_airport_code_origin", "wlif_airport_code_destination", "wlif_date_time_event", "wlif_date_time_received")
  val radius = Seq("wlif_session_id", "wlif_user", "wlif_account_type", "wlif_aircraft_code", "wlif_flight_id", "wlif_airline_code", "wlif_flight_number", "wlif_airport_code_origin", "wlif_airport_code_destination", "wlif_session_stop", "wlif_session_time", "wlif_in_volume", "wlif_out_volume")
}
