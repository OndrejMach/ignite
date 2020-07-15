package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.MAX_DATE
import com.tmobile.sit.ignite.rcse.structures.{CommonStructures, Terminal}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, upper}

class LookupsData(settings: Settings)(implicit sparkSession: SparkSession) extends Logger {
 import sparkSession.implicits._

  val client = CSVReader(
    path = settings.clientPath,
    header = false,
    schema = Some(CommonStructures.clientSchema),
    timestampFormat = "yyyy-MM-dd HH:mm:ss",
    delimiter = "|"
  ).read()

  val tac = CSVReader(
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
    timestampFormat = "yyyy-MM-dd HH:mm:ss")
    .read()
    .filter($"modification_date".isNotNull)
    .withColumn("rcse_terminal_sw_desc", upper($"rcse_terminal_sw_desc"))

}
