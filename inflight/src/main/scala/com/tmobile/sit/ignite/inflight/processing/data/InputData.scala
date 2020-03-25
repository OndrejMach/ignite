package com.tmobile.sit.ignite.inflight.processing.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.inflight.config.InputFiles
import com.tmobile.sit.ignite.inflight.datastructures.{InputStructures, InputTypes}
import org.apache.spark.sql.SparkSession

class InputData(input: InputFiles)(implicit sparkSession: SparkSession) extends Logger {
  val radius = {
    val path = input.path.get + input.radiusFile.get
    logger.info(s"Reading file ${path}")
    CSVReader(path,
      header = false,
      schema = Some(InputStructures.radiusStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )
      .read()
  }

  val aircraft = {
    import sparkSession.implicits._
    val path = input.path.get+input.aircraftFile.get
    logger.info(s"Reading file ${path}")
    CSVReader(input.path.get+input.aircraftFile.get,
      header = false,
      schema = Some(InputStructures.aircraftStruct)
      , delimiter = "|").read().as[InputTypes.Aircraft]
  }

  val airline = {
    import sparkSession.implicits._
    val path = input.path.get + input.airlineFile.get
    logger.info(s"Reading file ${path}")
    CSVReader(input.path.get + input.airlineFile.get,
      header = false,
      schema = Some(InputStructures.airlineStructure)
      ,delimiter = "|")
      .read().as[InputTypes.Airline]
  }

  val airport  = {
    import sparkSession.implicits._
    val path = input.path.get + input.airportFile.get
    logger.info(s"Reading file ${path}")
    CSVReader(input.path.get + input.airportFile.get,
      header = false,
      schema = Some(InputStructures.airportStructure)
      , delimiter = "|")
      .read().as[InputTypes.Airport]
  }

  val oooi  = {
    import sparkSession.implicits._
    val path = input.path.get + input.oooidFile.get
    logger.info(s"Reading file ${path}")
    CSVReader(input.path.get + input.oooidFile.get,
      header = false,
      schema = Some(InputStructures.oooidStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )
      .read().as[InputTypes.Oooid]
  }

  val flightLeg  = {
    import sparkSession.implicits._
    val path = input.path.get + input.flightlegFile.get
    logger.info(s"Reading file ${path}")
    CSVReader(input.path.get + input.flightlegFile.get,
      header = false,
      schema = Some(InputStructures.flightLegStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )
      .read()
  }

  val realm  = {
    import sparkSession.implicits._
    val path = input.path.get + input.realmFile.get
    logger.info(s"Reading file ${path}")
    CSVReader(input.path.get + input.realmFile.get,
      header = false,
      schema = Some(InputStructures.realmStructure)
      , delimiter = "|")
      .read().as[InputTypes.Realm]
  }
}
