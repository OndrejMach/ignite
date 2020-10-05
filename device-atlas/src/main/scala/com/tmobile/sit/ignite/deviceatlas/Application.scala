package com.tmobile.sit.ignite.deviceatlas

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.deviceatlas.config.Setup
import com.tmobile.sit.ignite.deviceatlas.data.{InputData, LookupData}
import com.tmobile.sit.ignite.deviceatlas.pipeline.{CoreProcessing,Pipeline, ResultWriter}

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

    var ODATE : String = ""
    var file_name_argument : String = ""
    if (args.length != 2) {
      logger.error("Incorrect number of parameters. Required options: -date=<date yyyyMMdd> -file=<file_name_argument>")
      System.exit(1)
    }
    for (arg <- args){
      if(arg.split("=").length != 2){
        logger.error("Incorrect argument format. Options: -date=<date yyyyMMdd> -file=<file_name_argument>")
        System.exit(1)
      } else {
        arg.split("=")(0) match {
          case "-date" => ODATE = arg.split("=")(1)
          case "-file" => file_name_argument = arg.split("=")(1)
          case _ => {logger.error(s"Unknown argument '$arg'. Required options: -date=<date yyyyMMdd> -file=<file_name_argument>")
            System.exit(1)}
        }
      }
    }
    logger.info(s"Job arguments -> processed file: '$file_name_argument', processing date: '$ODATE'")

    logger.info("Detecting operating system")
    val configFile = if(System.getProperty("os.name").startsWith("Windows")) {
      logger.info(s"Detected development configuration (${System.getProperty("os.name")})")
      "device-atlas-dev.conf"
    } else {
      logger.info(s"Detected production configuration (${System.getProperty("os.name")})")
      "device-atlas-prod"
    }

    logger.info(s"Reading configuration file")
    val setup = new Setup(configFile)
    logger.info(s"Configuration parameters check")
    if (!setup.settings.isAllDefined) {
      logger.error("Application parameters not properly defined")
      setup.settings.printMissingFields()
    }
    logger.info("Configuration parameters OK")
    setup.settings.printAllFields()

    logger.info("Getting SparkSession")
    implicit val settings = setup.settings
    implicit val sparkSession = getSparkSession(settings)

    //println("Web UI:")
    //println(sparkSession.sparkContext.uiWebUrl.get)

    // Inputs and lookups
    val lookups = new LookupData(settings.lookupPath.get)
    val input = new InputData(settings.inputPath.get, file_name_argument)

    // Core processing
    val processingCore = new CoreProcessing()

    // Results writer
    val resultWriter = new ResultWriter(settings)

    // Prepare orchestration pipeline
    val pipeline = new Pipeline(input,lookups,processingCore,resultWriter, settings, ODATE)

    // Run and finish
    pipeline.run()

    logger.info("Processing finished")
  }
}
