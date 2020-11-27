package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.Setup
import com.tmobile.sit.ignite.rcseu.data.{FileSchemas, InputData, PersistentData, ResultPaths}
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
  val (year, monthNum, dayNum) = (splitted(0), splitted(1),splitted(2))
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

  val inputReaders = InputData(
    activity = new CSVReader(conf.settings.inputPath.get + s"activity_${date}_${natco}.csv.gz", schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read(),
    provision = new CSVReader(conf.settings.inputPath.get + s"provision_${date}_${natco}.csv.gz", header = true, delimiter = "\t").read(),
    register_requests = new CSVReader(conf.settings.inputPath.get + s"register_requests_${date}_${natco}.csv.gz", header = true, delimiter = "\t").read()
  )
  logger.info("Activity file loaded: " + conf.settings.inputPath.get + s"activity_${date}_${natco}.csv.gz")

  val persistentData = PersistentData(
    oldUserAgents = new CSVReader(conf.settings.outputPath.get + "User_agents.csv", header = true, delimiter = "\t").read(),
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
