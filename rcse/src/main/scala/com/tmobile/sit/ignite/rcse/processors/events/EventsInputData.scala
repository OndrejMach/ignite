package com.tmobile.sit.ignite.rcse.processors.events

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


  val client = CSVReader(
    path = settings.clientPath,
    header = false,
    schema = Some(CommonStructures.clientSchema),
    timestampFormat = "yyyy-MM-DD HH:mm:ss",
    delimiter = "|"
  ).read()

  val tacTerminal = CSVReader(
    path = settings.tacPath,
    header = false,
    schema = Some(Terminal.tac_struct),
    delimiter = "|"
  ).read()
    .filter($"valid_to" >= lit(MAX_DATE))
    .withColumn("terminal_id", $"id")

  val terminal = CSVReader(path = settings.terminalPath,
    header = false,
    schema = Some(Terminal.terminal_d_struct),
    delimiter = "|")
    .read()



  val terminalSW = CSVReader(path = settings.terminalSWPath,
    header = false,
    schema = Some(CommonStructures.terminalSWSchema),
    delimiter = "|",
    timestampFormat = "yyyy-MM-DD HH:mm:ss")
    .read()
    .withColumn("rcse_terminal_sw_desc", upper($"rcse_terminal_sw_desc"))

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
