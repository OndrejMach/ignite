package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.Setup
import com.tmobile.sit.ignite.rcseu.data.{FileSchemas, InputData, PersistentData}
import com.tmobile.sit.ignite.rcseu.pipeline.{Core, Helper, Pipeline, ResultWriter}
import com.tmobile.sit.ignite.rcseu.pipeline.Stage

object Application extends App with Logger {

  //TODO: implement better flags like run-daily, run-monthly, run-yearly, run-all, run-debug
  if(args.length != 3) {
    logger.error("No arguments specified. Usage: ... <date:yyyy-mm-dd> <natco:cc> <isHistoric:bool>")
    System.exit(0)
  }

  // DEBUG variable to be used in other methods
  val debug = false;

  //TODO: natco network for Macedonia; move this out
  // variables needed in FactsProcesing and ProcessingCore for filtering
  val date = args(0)
  val natco = args(1)
  val isHistoric = args(2).toBoolean

  val date_split = date.split('-')
  val (year, monthNum, dayNum) = (date_split(0), date_split(1),date_split(2))
  val month = year + "-" + monthNum

  val monthforkey = year + "\\" +monthNum
  val dayforkey =  dayNum +"-"+ monthNum +"-"+ year

  val dateforoutput = year+monthNum+dayNum
  val monthforoutput = year+monthNum

  val mtID="1"
  val stID="3"
  val cgID="2"
  val crID="4"
  val mkID="7"

  val natcoID = if (natco == "mt") mtID
  else if (natco == "st") stID
  else if (natco == "cr") crID
  else if (natco == "cg") cgID
  else if (natco == "mk") mkID
  else "natco ID is not correct"

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

  val h = new Helper()
  // Use helper to reprocess historical data
  val activityFilePath = h.resolvePath(conf.settings, date, natco, isHistoric, "activity")
  val registerFilePath = h.resolvePath(conf.settings, date, natco, isHistoric, "register_requests")
  val provisionFilePath = h.resolvePath(conf.settings, date, natco, isHistoric, "provision")

  val inputReaders = InputData(
    activity = new CSVReader(activityFilePath, schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read(),
    provision = new CSVReader(provisionFilePath,schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t").read(),
    register_requests = new CSVReader(registerFilePath, schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t").read()
  )
  logger.info("Source files loaded")

  val persistentData = PersistentData(
    oldUserAgents = new CSVReader(conf.settings.lookupPath.get + "User_agents.csv", header = true, delimiter = "\t").read(),

    //TODO: see if this works with CSVReader class class for consistency
    activity_archives = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter","\\t")
      .schema(FileSchemas.activitySchema)
      .load(conf.settings.archivePath.get + s"activity*${year}*${natco}.csv*"),

    provision_archives = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter","\\t")
      .schema(FileSchemas.provisionSchema)
      .load(conf.settings.archivePath.get + s"provision*${year}*${natco}.csv*"),

    register_requests_archives = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter","\\t")
      .schema(FileSchemas.registerRequestsSchema)
      .load(conf.settings.archivePath.get + s"register_requests*${year}*${natco}*.csv*")
  )

  logger.info("Persistent files loaded")

  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultWriter = new ResultWriter(conf.settings)

  logger.info("Running pipeline")

  val pipeline = new Pipeline(inputReaders,persistentData,stageProcessing,coreProcessing,resultWriter)

  pipeline.run()

}
