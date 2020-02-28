package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.ignite.inflight.datastructures.InputStructures
import com.tmobile.sit.ignite.inflight.datastructures.StageStructures
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class StageProcess(implicit sparkSession: SparkSession) {
  def processAircraft(aircraftData: Dataset[InputStructures.Aircraft]): Dataset[StageStructures.Aircraft] = {
    import sparkSession.implicits._
    aircraftData.map(i =>
      StageStructures.Aircraft(
        wlif_aircraft_code = i.tailsign.getOrElse(""),
        wlif_aircraft_desc = if (i.name.isDefined && !i.name.get.isEmpty && i.name.get.compareTo("\u00A0") == 0) i.name.get.trim /*.substring(0,100) */ else "N/A",
        wlif_manufacturer = i.manufacturer.getOrElse("").trim() /*.substring(1,100)*/ ,
        wlif_ac_type = i.ac_type.getOrElse(""),
        wlif_serial_number = i.serial_number,
        wlif_year_of_manufacture = i.year_of_manufacture.getOrElse(0),
        wlif_airline_code = i.airline.getOrElse(""),
        wlif_icao_type = i.icao_type.get.trim /*.substring(1,100)*/ ,
        wlif_iata_type = i.iata_type.get.trim /*.substring(1,100)*/ ,
        wlif_gcs_equipped = if (i.gcs_equipped.isDefined && !i.gcs_equipped.get.isEmpty) i.gcs_equipped.get.trim else "no",
        wlif_xid_pac = i.xid.getOrElse(0),
        wlan_hotspot_ident_code = i.hotspot_id.getOrElse("").trim
      )
    )
  }

  /*
  out->wlif_airline_code        = in->airline_icao;
*out->wlif_airline_desc       = (in->airline_name == nullptr) ? "N/A" : *in->airline_name;
out->wlif_airline_iata        = in->airline_iata;
out->wlif_airline_logo_file   = in->airline_logo_file;
   */
  def processAirline(airlineInput: Dataset[InputStructures.Airline]): Dataset[StageStructures.Airline] = {
    import sparkSession.implicits._

    airlineInput.map(i => StageStructures.Airline(
      wlif_airline_code = i.airline_icao,
      wlif_airline_desc = i.airline_name.getOrElse("N/A"),
      wlif_airline_iata = i.airline_iata,
      wlif_airline_logo_file = i.airline_logo_file
    )
    )
  }
/*
out->wlif_airport_code           = in->airport_icao;
*out->wlif_airport_desc          = (in->airport_name == nullptr || trim(*in->airport_name).empty()) ? "N/A" : trim(*in->airport_name);
out->wlif_iata                   = in->airport_iata;
out->wlif_city                   = in->airport_city;
out->wlif_country                = in->airport_country;
out->wlif_latitude               = in->airport_latitude;
out->wlif_longitude              = in->airport_longitude;
out->wlif_coverage               = in->airport_coverage;
 */
  def processAirport(airportInput: Dataset[InputStructures.Airport]) : Dataset[StageStructures.Airport] = {
    import sparkSession.implicits._

    airportInput.map(i=> StageStructures.Airport(
      wlif_airport_code = i.airport_icao,
      wlif_airport_desc = if (!i.airport_name.isDefined || i.airport_name.get.trim.isEmpty) "N/A" else i.airport_name.get.trim,
      wlif_iata = i.airport_iata,
      wlif_city = i.airport_city,
      wlif_country = i.airport_country,
      wlif_latitude = i.airport_latitude,
      wlif_longitude = i.airport_longitude,
      wlif_coverage = i.airport_coverage
    ))
  }

/*
out->wlif_realm_code    = in->realm_prefix;
out->wlif_realm_desc    = in->realm_prefix;
out->wlif_account_type  = in->account_type;
 */
  def preprocessReal(realmInput: Dataset[InputStructures.Realm]) : Dataset[StageStructures.Realm] = {
    import sparkSession.implicits._

    realmInput.map(i => StageStructures.Realm(
      wlif_realm_code = i.realm_prefix,
      wlif_realm_desc = i.realm_prefix,
      wlif_account_type = i.account_type
    ))
  }

 /*
 out->wlif_sequence               = in->wlif_sequence;
*out->wlif_method                = trim(*in->wlif_method).substr(0,100);
out->wlif_flight_id              = in->wlif_flight_id;
*out->wlif_auid                  = trim(*in->wlif_auid).substr(0,100);
out->wlif_xid_pac                = in->wlif_xid_pac;
out->wlif_airline_code           = in->wlif_airline_code;
out->wlif_aircraft_code          = in->wlif_aircraft_code;
out->wlif_flight_number          = in->wlif_flight_number;
out->wlif_airport_code_origin    = in->wlif_airport_code_origin;
out->wlif_airport_code_destination = in->wlif_airport_code_destination;
out->wlif_date_time_event        = in->wlif_date_time_event;
out->wlif_date_time_received     = in->wlif_date_time_received;
*out->entry_id                   = run_id;
*out->load_date                  = load_date;
  */

  def processOooid(oooidInput: Dataset[InputStructures.Oooid], runId: Int = 0, loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now())) : Dataset[StageStructures.Oooi] = {
    import sparkSession.implicits._


    oooidInput.map(
      i=> StageStructures.Oooi(
        wlif_sequence = i.wlif_sequence,
        wlif_method = i.wlif_method,
        wlif_flight_id = i.wlif_flight_id,
        wlif_auid = i.wlif_auid,
        wlif_xid_pac = i.wlif_xid_pac,
        wlif_airline_code = i.wlif_airline_code,
        wlif_aircraft_code = i.wlif_aircraft_code,
        wlif_flight_number = i.wlif_flight_number,
        wlif_airport_code_origin = i.wlif_airport_code_origin,
        wlif_airport_code_destination = i.wlif_airport_code_destination,
        wlif_date_time_event = i.wlif_date_time_event,
        wlif_date_time_received = i.wlif_date_time_received,
        entry_id = Some(runId),
        load_date = Some(loadDate))
      )
  }



}
