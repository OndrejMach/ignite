package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.terminald.UpdateDTerminal
import com.tmobile.sit.ignite.rcse.structures.Terminal
import org.apache.spark.sql.SparkSession

class TerminalDProcessor(settings: Settings)(implicit sparkSession: SparkSession) extends Processor {
  override def processData(): Unit = {
    val terminalDData = CSVReader(path = settings.terminalPath,
      header = false,
      schema = Some(Terminal.terminal_d_struct),
      delimiter = "|")
      .read()

    val tac = CSVReader(
      path = settings.tacPath,
      header = false,
      schema = Some(Terminal.tac_struct),
      delimiter = "|"
    ).read()

    val terminalDResultData = new UpdateDTerminal(terminalDData, tac, settings.maxDate).getData()

    //TODO quotation
    CSVWriter(
      data = terminalDResultData,
      path = settings.outputPath,
      delimiter = "|"
    ).writeData()
  }

}
