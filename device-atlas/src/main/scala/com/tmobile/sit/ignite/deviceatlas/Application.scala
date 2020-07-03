package com.tmobile.sit.ignite.deviceatlas

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.deviceatlas.config.Setup
import com.tmobile.sit.ignite.deviceatlas.pipeline.{Dimensions, ExportOutputs, TerminalDB}
import com.tmobile.sit.ignite.deviceatlas.processing.data.{InputData, LookupData, OutputData}

/**
 * processing starts here:
 * 1) reads and validates configuration files, initialised logger, prints parameters
 * 2) initialises spark session
 * 3) process command line arguments
 * 4) read lookup and input files
 * 5) generates updated data
 * 6) write output files
 *
 * expected command line parameters:
 *  -date : processing date, only affects valid_to values and timestamp on output files
 *  -file : name of a file to process
 */

object Application extends Logger{

  def main(args: Array[String]): Unit = {

    logger.info(s"Reading configuration file")
    val setup = new Setup()
    logger.info(s"Configuration parameters check")
    if (!setup.settings.isAllDefined) {
      logger.error("Application parameters not properly defined")
      setup.settings.printMissingFields()
    }
    logger.info("Configuration parameters OK")
    setup.settings.printAllFields()

    logger.info("Getting SparkSession")
    implicit val sparkSession = getSparkSession(setup.settings.appName.get)
    implicit val settings = setup.settings

    println("Web UI: " + sparkSession.sparkContext.uiWebUrl.get)

    var ODATE : String = ""
    var file_name : String = ""
    if (args.length != 2) {
      logger.error("Incorrect number of parameters. Required options: -date=<date yyyyMMdd> -file=<file_name>")
      System.exit(1)
    }
    for (arg <- args){
      if(arg.split("=").length != 2){
        logger.error("Incorrect argument format. Options: -date=<date yyyyMMdd> -file=<file_name>")
        System.exit(1)
      } else {
        arg.split("=")(0) match {
          case "-date" => ODATE = arg.split("=")(1)
          case "-file" => file_name = arg.split("=")(1)
          case _ => {logger.error(s"Unknown argument '$arg'. Required options: -date=<date yyyyMMdd>] -file=<file_name>")
            System.exit(1)}
        }
      }
    }
    logger.info(s"Job arguments -> processed file: '$file_name', processing date: '$ODATE'")

    val lookups = new LookupData(settings.lookupPath.get)
    val input = new InputData(settings.inputPath.get, file_name)

    val terminalDBupdate = new TerminalDB()
    val dimFiles = new Dimensions()
    val files_to_send = new ExportOutputs()

    // for testing/limited run purposes
    /*
       val updatedTerminalDB = {
          val file = settings.outputPath.get + "terminaldb_20200602.csv"
          logger.info(s"Reading file: $file")
          CSVReader(file,
            header = false,
            schema = Some(FileStructures.terminalDB_full_lkp),
            delimiter = "|")
            .read()
        }

      val updatedD_terminal = {
      val file = settings.outputPath.get + "cptm_ta_d_terminal_spec.tmp"
      logger.info(s"Reading file: ${file}")
      CSVReader(file,
        header = false,
        schema = Some(FileStructures.cptm_term_spec),
        delimiter = "|")
        .read()
    }

        val updatedD_tac = {
          val file = settings.outputPath.get + "cptm_ta_d_tac.tmp"
          logger.info(s"Reading file: ${file}")
          CSVReader(file,
            header = false,
            schema = Some(FileStructures.cptm_ta_d_tac),
            delimiter = "|")
            .read()
        }
    */

    val updatedTerminalDB = terminalDBupdate.updateTerminalDB(input, lookups, ODATE, settings.outputPath.get).cache()
    val updatedD_terminal = dimFiles.update_d_terminal(updatedTerminalDB, input.d_terminal_spec, settings.workPath.get, ODATE).cache()
    val updatedD_tac = dimFiles.update_d_tac(updatedTerminalDB, input.d_tac, settings.workPath.get, ODATE).cache()
    val cptm_ta_d_terminal_spec = files_to_send.generateSpec(updatedD_terminal, ODATE, settings.outputPath.get)
    val cptm_vi_d_tac_terminal = files_to_send.generateTac(updatedD_tac, ODATE, settings.outputPath.get)

    OutputData(updatedTerminalDB,
      updatedD_terminal,
      updatedD_tac,
      cptm_vi_d_tac_terminal,
      cptm_ta_d_terminal_spec)
    .write(settings.outputPath.get, ODATE)

    /*
        logger.info(s"Writing ${settings.outputPath.get}cptm_ta_d_terminal_spec.tmp")
        CSVWriter(data = updatedD_terminal,
          path = s"${settings.outputPath.get}cptm_ta_d_terminal_spec.tmp",  delimiter = "|", writeHeader = false, escape = "", quote = "").writeData()

        logger.info(s"Writing ${settings.outputPath.get}cptm_ta_d_tac.tmp")
        CSVWriter(data = updatedD_tac,
          path = s"${settings.outputPath.get}cptm_ta_d_tac.tmp",  delimiter = "|", writeHeader = false).writeData()

        logger.info(s"Writing ${settings.outputPath.get}cptm_vi_d_tac_terminal_${ODATE}.csv")
        CSVWriter(data = cptm_vi_d_tac_terminal,
          path = s"${settings.outputPath.get}cptm_vi_d_tac_terminal_${ODATE}.csv",  delimiter = ";", writeHeader = false).writeData()

        logger.info(s"Writing ${settings.outputPath.get}cptm_ta_d_terminal_spec_${ODATE}.csv")
        CSVWriter(data = cptm_ta_d_terminal_spec,
          path = s"${settings.outputPath.get}cptm_ta_d_terminal_spec_${ODATE}.csv",  delimiter = ";", writeHeader = false).writeData()
    */

  } // def main(...){
}
