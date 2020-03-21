package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.ignite.inflight.datastructures.InputTypes
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.lit

class StageProcess(implicit sparkSession: SparkSession) {
  def processAircraft(aircraftData: Dataset[InputTypes.Aircraft]): Dataset[StageTypes.Aircraft] = {
    import sparkSession.implicits._
    aircraftData.map(i =>
      StageTypes.Aircraft(
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

  def processAirline(airlineInput: Dataset[InputTypes.Airline]): Dataset[StageTypes.Airline] = {
    import sparkSession.implicits._

    airlineInput.map(i => StageTypes.Airline(
      wlif_airline_code = i.airline_icao,
      wlif_airline_desc = i.airline_name.getOrElse("N/A"),
      wlif_airline_iata = i.airline_iata,
      wlif_airline_logo_file = i.airline_logo_file
    )
    )
  }

  def processAirport(airportInput: Dataset[InputTypes.Airport]): Dataset[StageTypes.Airport] = {
    import sparkSession.implicits._

    airportInput.map(i => StageTypes.Airport(
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
  def preprocessRealm(realmInput: Dataset[InputTypes.Realm]): Dataset[StageTypes.Realm] = {
    import sparkSession.implicits._

    realmInput.map(i => StageTypes.Realm(
      wlif_realm_code = i.realm_prefix,
      wlif_realm_desc = i.realm_prefix,
      wlif_account_type = i.account_type
    ))
  }

  def processOooid(oooidInput: Dataset[InputTypes.Oooid], runId: Int = 0, loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now())): Dataset[StageTypes.Oooi] = {
    import sparkSession.implicits._


    oooidInput.map(
      i => StageTypes.Oooi(
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
        //TODO review
        entry_id = Some(runId),
        load_date = Some(loadDate))
    )
  }

  def processRadius(inputRadius: DataFrame, entryId: Int = 0, loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now())): Dataset[StageTypes.Radius] = {
    import sparkSession.implicits._
    inputRadius
      .withColumn("entry_id", lit(entryId))
      .withColumn("load_date", lit(loadDate))
      .as[StageTypes.Radius]
  }

  def processFlightLeg(inputFlightLeg: DataFrame, entryId: Int = 0, loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now())) : Dataset[StageTypes.FlightLeg] = {
    import sparkSession.implicits._
    inputFlightLeg
      .withColumn("entry_id", lit(entryId))
      .withColumn("load_date", lit(loadDate))
      .as[StageTypes.FlightLeg]
  }




}
