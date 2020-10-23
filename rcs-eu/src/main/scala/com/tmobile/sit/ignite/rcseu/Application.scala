package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.Setup
import com.tmobile.sit.ignite.rcseu.data.{InputData, PersistentData, ResultPaths}
import com.tmobile.sit.ignite.rcseu.pipeline.{Core, Pipeline, ResultWriter}
import com.tmobile.sit.ignite.rcseu.pipeline.Stage


object Application extends App with Logger {

  if(args.length != 3) {
    logger.error("No arguments specified. Usage: ... <date> <natco>")
    System.exit(0)
  }
  //TODO: natco network for Macedonia
// variables needed in FactsProcesing and ProcessingCore for filtering
  val date = args(0)
  val natco = args(1)
  val isHistoric = args(2).toBoolean

  val splitted = date.split('-')
  val (year, monthNum) = (splitted(0), splitted(1))
  val month = year + "-" + monthNum

  val mt="dt-magyar-telecom"
  val st="dt-slovak-telecom"
  val cg="dt-cosmote-greece"
  val cr="dt-telecom-romania"
  val mk="-"

  val natcoNetwork = if (natco == "mt") mt
                    else if (natco == "st") st
                    else if (natco == "cr") cr
                    else if (natco == "cg") cg
                    else if (natco == "mk") mk
  else "natco network is not correct"

  logger.info(s"Date: $date, month:$month, year:$year, natco: $natco, natcoNetwork: $natcoNetwork, isHistoric: $isHistoric")

  val configFile = if(System.getProperty("os.name").startsWith("Windows")) {
    logger.info(s"Detected development configuration (${System.getProperty("os.name")})")
    "rcs-eu.windows.conf"
  } else {
    logger.info(s"Detected production configuration (${System.getProperty("os.name")})")
    "rcs-eu.linux.conf"
  }


  logger.info("Configuration setup for " + configFile)
  val conf = new Setup(configFile)

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()

  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  val inputReaders = InputData(
    activity = new CSVReader(conf.settings.inputPath.get + s"activity_${date}_${natco}.csv.gz", header = true, delimiter = "\t").read(),
    provision = new CSVReader(conf.settings.inputPath.get + s"provision_${date}_${natco}.csv.gz", header = true, delimiter = "\t").read(),
    register_requests = new CSVReader(conf.settings.inputPath.get + s"register_requests_${date}_${natco}.csv.gz", header = true, delimiter = "\t").read()
  )

  val persistentData = PersistentData(
    oldUserAgents = new CSVReader(conf.settings.outputPath.get + "UserAgents.csv", header = true, delimiter = ";").read(),
    accumulated_activity =  sparkSession.read.parquet(conf.settings.lookupPath.get + "acc_activity.parquet"),
    accumulated_provision =  sparkSession.read.parquet(conf.settings.lookupPath.get + "acc_provision.parquet"),
    accumulated_register_requests =  sparkSession.read.parquet(conf.settings.lookupPath.get + "acc_register_requests.parquet")

    //accumulated_activity = new CSVReader(conf.settings.lookupPath.get + "acc_activity.csv", header = true, delimiter = ";").read(),
    //accumulated_provision = new CSVReader(conf.settings.lookupPath.get + "acc_provision.csv", header = true, delimiter = ";").read(),
    //accumulated_register_requests = new CSVReader(conf.settings.lookupPath.get + "acc_register_requests.csv", header = true, delimiter = ";").read()

  )

  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultPaths = ResultPaths(conf.settings.lookupPath.get, conf.settings.outputPath.get)
  val resultWriter = new ResultWriter(resultPaths)

  val pipeline = new Pipeline(inputReaders,persistentData,stageProcessing,coreProcessing,resultWriter)

  pipeline.run()

}
