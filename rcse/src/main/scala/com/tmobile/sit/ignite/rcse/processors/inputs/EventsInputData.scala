package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{CommonStructures, Events}
import org.apache.spark.sql.SparkSession

class EventsInputData(implicit sparkSession: SparkSession,settings: Settings) extends Logger {
  val dataInput = {
    logger.info(s"Reading data from ${settings.app.inputFilesPath}")
    CSVReader(
    path = settings.app.inputFilesPath,
    header = false,
    schema = Some(Events.eventsSchema),
    timestampFormat = "yyyyMMddHHmmss",
    delimiter = "|"
  ).read()
  }


  val imsi3DesLookup = {
    logger.info(s"Reading data from ${settings.stage.imsisEncodedPath}")
    CSVReader(path = settings.stage.imsisEncodedPath,
      header = false,
      schema = Some(CommonStructures.des3Schema),
      delimiter = ",")
      .read()
  }

  val msisdn3DesLookup = {
    logger.info(s"Reading data from ${settings.stage.msisdnsEncodedPath}")
    CSVReader(path = settings.stage.msisdnsEncodedPath,
      header = false,
      schema = Some(CommonStructures.des3Schema),
      delimiter = ",")
      .read()
  }
}
