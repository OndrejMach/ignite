package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.terminald.UpdateDTerminal
import com.tmobile.sit.ignite.rcse.structures.Terminal
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class TerminalDProcessor(implicit sparkSession: SparkSession,settings: Settings ) extends Logger {
   def processData(): DataFrame = {
    val terminalDData = {
      logger.info(s"Reading data from ${settings.stage.terminalPath}")
      CSVReader(path = settings.stage.terminalPath,
        header = false,
        schema = Some(Terminal.terminal_d_struct),
        delimiter = "|")
        .read()
    }

    val tac = {
      logger.info(s"Reading data from ${settings.stage.tacPath}")
      CSVReader(
        path = settings.stage.tacPath,
        header = false,
        schema = Some(Terminal.tac_struct),
        delimiter = "|"
      ).read()
    }

    new UpdateDTerminal(terminalDData, tac, settings.app.maxDate).getData()

    /*
    //TODO quotation
    terminalDResultData
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "true")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .csv(settings.outputPath);

    CSVWriter(
      data = terminalDResultData
        .na
        .fill("", Seq("rcse_terminal_id", "tac_code",
          "terminal_id", "rcse_terminal_vendor_sdesc",
          "rcse_terminal_vendor_ldesc", "rcse_terminal_model_sdesc",
        "rcse_terminal_model_ldesc", "modification_date")),
      path = settings.outputPath,
      delimiter = "|",
      quoteMode = "NONE"
    ).writeData()

 */
  }

}
