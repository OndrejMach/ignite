package com.tmobile.sit.ignite.inflight.processing.data

import com.tmobile.sit.ignite.inflight.datastructures.StageTypes
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}

/**
 * Here input data is verified, filtered or slightly transformed if needed. This is basically the source for processing.
 *
 * @param input        - class containing raw inputs
 * @param sparkSession - yes
 */
class StageData(input: InputData)(implicit sparkSession: SparkSession) {
  val aircraft: Dataset[StageTypes.Aircraft] = {
    import sparkSession.implicits._
    input.aircraft.map(i =>
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

  val airline: Dataset[StageTypes.Airline] = {
    import sparkSession.implicits._

    input.airline.map(i => StageTypes.Airline(
      wlif_airline_code = i.airline_icao,
      wlif_airline_desc = i.airline_name.getOrElse("N/A"),
      wlif_airline_iata = i.airline_iata,
      wlif_airline_logo_file = i.airline_logo_file
    )
    )
  }

  val airport: Dataset[StageTypes.Airport] = {
    import sparkSession.implicits._

    input.airport.map(i => StageTypes.Airport(
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
  val realm: Dataset[StageTypes.Realm] = {
    import sparkSession.implicits._

    input.realm.map(i => StageTypes.Realm(
      wlif_realm_code = i.realm_prefix,
      wlif_realm_desc = i.realm_prefix,
      wlif_account_type = i.account_type
    ))
  }

  val oooi: Dataset[StageTypes.Oooi] = {
    import sparkSession.implicits._
    input.oooi
      //.withColumn("entry_id", lit(runId))
      // .withColumn( "load_date", lit(loadDate))
      .as[StageTypes.Oooi]
  }

  val radius: Dataset[StageTypes.Radius] = {
    import sparkSession.implicits._
    input.radius
      // .withColumn("entry_id", lit(runId))
      // .withColumn("load_date", lit(loadDate))
      .as[StageTypes.Radius]
  }

  val flightLeg: Dataset[StageTypes.FlightLeg] = {
    import sparkSession.implicits._

    val ret = input.flightLeg
      .withColumn("wlif_num_users", when(col("wlif_num_users").equalTo("\\N"), lit(null).cast(LongType)).otherwise(col("wlif_num_users").cast(LongType)))
      .withColumn("wlif_num_sessions", when(col("wlif_num_sessions").equalTo("\\N"), lit(null).cast(LongType)).otherwise(col("wlif_num_sessions").cast(LongType)))
      .withColumn("wlif_session_time", when(col("wlif_session_time").equalTo("\\N"), lit(null).cast(LongType)).otherwise(col("wlif_session_time").cast(LongType)))
      .withColumn("wlif_session_volume_out", when(col("wlif_session_volume_out").equalTo("\\N"), lit(null).cast(DoubleType)).otherwise(col("wlif_session_volume_out").cast(DoubleType)))
      .withColumn("wlif_session_volume_in", when(col("wlif_session_volume_in").equalTo("\\N"), lit(null).cast(DoubleType)).otherwise(col("wlif_session_volume_in").cast(DoubleType)))
      .withColumn("wlif_active_sessions", when(col("wlif_active_sessions").equalTo("\\N"), lit(null).cast(LongType)).otherwise(col("wlif_active_sessions").cast(LongType)))
      .as[StageTypes.FlightLeg]
    ret
  }
}
