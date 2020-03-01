package com.tmobile.sit.ignite.inflight

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.datastructures.InputStructures
import com.tmobile.sit.ignite.inflight.datastructures.StageStructures
import org.apache.spark.sql.SparkSession

object ReferenceData {
  val inputAircraft = List(InputStructures.Aircraft(Some("B-16732"),Some("Boeing"), Some("B777-300ER"), None, Some(0), None,Some("EVA"), Some("B773"), Some("773"),Some("yes"), Some(4996), Some("DE_TW5459")))
  val stagedAircraft = List(StageStructures.Aircraft("B-16732", "N/A", "Boeing","B777-300ER", None, 0, "EVA", "B773","773", "yes", 4996, "DE_TW5459"))

  val stagedAirline = List(StageStructures.Airline(Some("SCO"), "Scoot", Some("TZ"), None))

  val stagedAirport = List(StageStructures.Airport(Some("AYGA"),"GOROKA", Some("GKA"), Some("GOROKA"), Some("PAPUA NEW GUINE"), Some(-6.08167), Some(145.392), Some("no") ))

  val stagedRealm = List(StageStructures.Realm(Some("t-online.de"), Some("t-online.de"), Some("postpaid")))

  def stagedOooi (loadDate: Timestamp) = List(StageStructures.Oooi(Some(9842350), Some("arrive_gate"), Some(6800409), Some("0"), Some(55005), Some("ETD"), Some("A6-BLJ"), Some("418"),
    Some("WMKK"), Some("NNNN"), Some(Timestamp.valueOf("2019-10-27 02:26:00")), Some(Timestamp.valueOf("2019-10-27 02:30:27")), Some(0), Some(loadDate)))

  def radiusStage(loadDate: Timestamp) = List(
    StageStructures.Radius(
      wlif_session_id= Some("000000002D00000009883F2C"), wlif_user = Some("5443959ba539da2d1e3d9e943914fcb6265ba0d1ac5f1b971c261cd6"), wlif_username = Some("dabe76725083995debbf3281656f7665c3968bc0eda96f7a616ec34e"),
      wlif_realm_code = Some("hdrinc.com"), wlif_account_type= Some("ipass_roam"), wlif_prefix = None, wlan_hotspot_ident_code = Some("DE_FY5059"), wlif_xid_pac = Some(2079),
      wlif_aircraft_code = Some("D-AIFE"), wlif_flight_id = Some(6910966), wlif_airline_code = Some("DLH"), wlif_flight_number = Some("445"), wlif_airport_code_origin = Some("KATL"),
      wlif_airport_code_destination = Some("EDDF"), wlif_session_start = Some(Timestamp.valueOf("2019-11-19 02:42:44")), wlif_session_stop = Some(Timestamp.valueOf("2019-11-19 04:42:45")),
      wlif_session_time = Some(7201), wlif_in_volume = Some(22897.7), wlif_out_volume = Some(101646.0), wlif_termination_cause = Some("Session-Timeout"), entry_id =0, load_date =loadDate
    )
  )

  def flightLegStage(loadDate: Timestamp) = List(
    StageStructures.FlightLeg(
      wlif_flight_id= Some(6482956), wlif_flightleg_status = Some("closed"), wlif_airline_code = Some("AAL"),
      wlif_aircraft_code = Some("N385AM"), wlif_flight_number= Some("700"), wlif_airport_code_origin = Some("KSFO"), wlif_airport_code_destination = Some("KPHL"), wlif_date_time_opened = Some(Timestamp.valueOf("2019-08-23 15:36:00")),
      wlif_method_opened = Some("takeoff"), wlif_date_time_closed = Some(Timestamp.valueOf("2019-08-23 20:21:00")), wlif_method_closed = Some("landing"), wlif_xid_pac = Some(0), wlif_num_users = Some(0),
      wlif_num_sessions = Some(0), wlif_session_time = Some(0), wlif_session_volume_out = Some(0.0),
      wlif_session_volume_in = Some(0.0), wlif_active_sessions = Some(0), entry_id =0, load_date =loadDate
    )
  )
}
