package com.tmobile.sit.ignite.inflight.processing.data

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.inflight.datastructures.{InputStructures, InputTypes}
import org.apache.spark.sql.SparkSession

object InputData {
  def radius(implicit sparkSession: SparkSession) = {
    CSVReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/inflight/data/input/G_2020-02-12_03-35-07_radius.csv",
      header = false,
      schema = Some(InputStructures.radiusStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )
      .read()
  }

  def aircraft(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/inflight/data/input/G_2020-02-12_03-35-07_aircraft.csv",
      header = false,
      schema = Some(InputStructures.aircraftStruct)
      , delimiter = "|").read().as[InputTypes.Aircraft]
  }

  def airline(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/inflight/data/input/G_2020-02-12_03-35-07_airline.csv",
      header = false,
      schema = Some(InputStructures.airlineStructure)
      ,delimiter = "|")
      .read().as[InputTypes.Airline]
  }

  def airport (implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/inflight/data/input/G_2020-02-12_03-35-07_airport.csv",
      header = false,
      schema = Some(InputStructures.airportStructure)
      , delimiter = "|")
      .read().as[InputTypes.Airport]
  }

  def oooi (implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/inflight/data/input/G_2020-02-12_03-35-07_oooi.csv",
      header = false,
      schema = Some(InputStructures.oooidStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )
      .read().as[InputTypes.Oooid]
  }

  def flightLeg (implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/inflight/data/input/G_2020-02-12_03-35-07_flightleg.csv",
      header = false,
      schema = Some(InputStructures.flightLegStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )
      .read()
  }

  def realm (implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/inflight/data/input/G_2020-02-12_03-35-07_account_type.csv",
      header = false,
      schema = Some(InputStructures.realmStructure)
      , delimiter = "|")
      .read().as[InputTypes.Realm]
  }
}
