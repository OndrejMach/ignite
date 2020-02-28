package com.tmobile.sit.ignite.inflight.datastructures

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.datastructures.InputStructures.Aircraft
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object InputStructures {

  /*
tailsign                string
manufacturer            string       null=""
ac_type                 string       null=""
serial_number           string       null=""
year_of_manufacture     int          null="0000"
name                    string       enc="utf8"
airline                 string       null=""
icao_type               string       null=""
iata_type               string       null=""
gcs_equipped            string       null=""
xid                     int
hotspot_id              string       null=""
 */
  val aircraftStruct = StructType(
    Seq(
      StructField("tailsign", StringType, true),
      StructField("manufacturer", StringType, true),
      StructField("ac_type", StringType, true),
      StructField("serial_number", StringType, true),
      StructField("year_of_manufacture", IntegerType, true),
      StructField("name", StringType, true),
      StructField("airline", StringType, true),
      StructField("icao_type", StringType, true),
      StructField("iata_type", StringType, true),
      StructField("gcs_equipped", StringType, true),
      StructField("xid", IntegerType, true),
      StructField("hotspot_id", StringType, true)
    )
  )

  case class Aircraft(tailsign: Option[String],
                      manufacturer: Option[String],
                      ac_type: Option[String],
                      serial_number: Option[String],
                      year_of_manufacture: Option[Int],
                      name: Option[String],
                      airline: Option[String],
                      icao_type: Option[String],
                      iata_type: Option[String],
                      gcs_equipped: Option[String],
                      xid: Option[Int],
                      hotspot_id: Option[String])

  /*
airline_name                string
airline_icao                string
airline_iata                string       null=""
airline_logo_file           string       null=""
 */
  val airlineStructure = StructType(Seq(
    StructField("airline_name", StringType, true),
    StructField("airline_icao", StringType, true),
    StructField("airline_iata", StringType, true),
    StructField("airline_logo_file", StringType, true)
  )

  )

  case class Airline(airline_name: Option[String],
                     airline_icao: Option[String],
                     airline_iata: Option[String],
                     airline_logo_file: Option[String]
                    )

  /*
airport_icao                string
airport_iata                string        null=""
airport_name                string
airport_city                string        null=""
airport_country             string        null=""
airport_latitude            decimal(18,8) null=""
airport_longitude           decimal(18,8) null=""
airport_coverage            string        null=""
 */

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

  case class Airport(airport_icao: Option[String],
                     airport_iata: Option[String],
                     airport_name: Option[String],
                     airport_city: Option[String],
                     airport_country: Option[String],
                     airport_latitude: Option[Double],
                     airport_longitude: Option[Double],
                     airport_coverage: Option[String]
                    )

  /*
wlif_flight_id                   int
wlif_flightleg_status            string        null=""
wlif_airline_code                string        null=""
wlif_aircraft_code               string        null=""
wlif_flight_number               string        null=""
wlif_airport_code_origin         string        null=""
wlif_airport_code_destination    string        null=""
wlif_date_time_opened            timestamp     null="0000-00-00 00:00:00"
wlif_method_opened               string        null=""
wlif_date_time_closed            timestamp     null="0000-00-00 00:00:00"
wlif_method_closed               string        null=""
wlif_xid_pac                     int
wlif_num_users                   int           null="\N"
wlif_num_sessions                int           null="\N"
wlif_session_time                int           null="\N"
wlif_session_volume_out          decimal(18,8) null="\N"
wlif_session_volume_in           decimal(18,8) null="\N"
wlif_active_sessions             int           null="\N"
 */
  case class AirportLeg(wlif_flight_id: Option[Int],
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
                        wlif_active_sessions: Option[Int]
                       )

  /*
wlif_sequence                    int
wlif_method                      string       null=""
wlif_flight_id                   int          null="\\N"
wlif_auid                        string       null=""
wlif_xid_pac                     int
wlif_airline_code                string       null=""
wlif_aircraft_code               string       null=""
wlif_flight_number               string       null=""
wlif_airport_code_origin         string       null=""
wlif_airport_code_destination    string       null=""
wlif_date_time_event             timestamp    null="0000-00-00 00:00:00"
wlif_date_time_received          timestamp    null="0000-00-00 00:00:00"
 */

  val oooidStructure = StructType(Seq(
    StructField("wlif_sequence", IntegerType, true),
    StructField("wlif_method", StringType, true),
    StructField("wlif_flight_id", IntegerType, true),
    StructField("wlif_auid", StringType, true),
    StructField("wlif_xid_pac", IntegerType, true),
    StructField("wlif_airline_code", StringType, true),
    StructField("wlif_aircraft_code", StringType, true),
    StructField("wlif_flight_number", StringType, true),
    StructField("wlif_airport_code_origin", StringType, true),
    StructField("wlif_airport_code_destination", StringType, true),
    StructField("wlif_date_time_event", TimestampType, true),
    StructField("wlif_date_time_received", TimestampType, true)
  )

  )
  case class Oooid(
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
                    wlif_date_time_received: Option[Timestamp]
                  )

  /*
realm_prefix                string
account_type                string      null=""
 */

  val realmStructure = StructType(Seq(
    StructField("realm_prefix", StringType, true),
    StructField("account_type", StringType, true)
  )
  )

  case class Realm(
                    realm_prefix: Option[String],
                    account_type: Option[String]
                  )

  /*
wlif_session_id                  string
wlif_user                        string        null=""
wlif_username                    string        null=""
wlif_realm_code                  string        null=""
wlif_account_type                string        null=""
wlif_prefix                      string        null=""
wlan_hotspot_ident_code          string        null=""
wlif_xid_pac                     int
wlif_aircraft_code               string        null=""
wlif_flight_id                   int           null=""
wlif_airline_code                string        null=""
wlif_flight_number               string        null=""
wlif_airport_code_origin         string        null=""
wlif_airport_code_destination    string        null=""
wlif_session_start               timestamp     null=""
wlif_session_stop                timestamp     null=""
wlif_session_time                int           null=""
wlif_in_volume                   decimal(18,8) null=""
wlif_out_volume                  decimal(18,8) null=""
wlif_termination_cause           string        null=""
 */

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
                     wlif_termination_cause: Option[String]

                   )

}