package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{CommonStructures, Events, Terminal}
import com.tmobile.sit.ignite.rcse.processors.MAX_DATE
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, upper}

class EventsInputData(settings: Settings)(implicit sparkSession: SparkSession) {
  import sparkSession.implicits._
  val dataInput = CSVReader(path = settings.inputFilesPath,
    header = false,
    schema = Some(Events.eventsSchema),
    timestampFormat = "yyyyMMddHHmmss",
    delimiter = "|"
  ) read()


  val imsi3DesLookup = CSVReader(path = settings.imsisEncodedPath,
    header = false,
    schema = Some(CommonStructures.des3Schema),
    delimiter = ",")
    .read()

  val msisdn3DesLookup = CSVReader(path = settings.msisdnsEncodedPath,
    header = false,
    schema = Some(CommonStructures.des3Schema),
    delimiter = ",")
    .read()
}
