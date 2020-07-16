package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.Setup
import com.tmobile.sit.ignite.rcseu.data.{InputData, PersistentData, ResultPaths}
import com.tmobile.sit.ignite.rcseu.pipeline.{Core, Pipeline, ResultWriter}
import com.tmobile.sit.ignite.rcseu.pipeline.Stage

object Application extends App with Logger {

  if(args.length != 2) {
    logger.error("No arguments specified. Usage: ... <date> <natco>")
    System.exit(0)
  }

  val date = args(0)
  val natco = args(1)

  logger.info(s"Date: $date, natco: $natco")

  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()

  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  val inputReaders = InputData(
    activity = new CSVReader(conf.settings.inputPath.get + s"activity_${date}_${natco}.csv.gz", header = true, delimiter = "\t"),
    provision = new CSVReader(conf.settings.inputPath.get + s"provision_${date}_${natco}.csv.gz", header = true, delimiter = "\t"),
    register_requests = new CSVReader(conf.settings.inputPath.get + s"register_requests_${date}_${natco}.csv.gz", header = true, delimiter = "\t")
  )

  val persistentData = PersistentData(
    oldUserAgents = new CSVReader(conf.settings.inputPath.get + "UserAgents.csv", header = true, delimiter = ";").read()
  )

  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultPaths = ResultPaths(conf.settings.lookupPath.get, conf.settings.outputPath.get)
  val resultWriter = new ResultWriter(resultPaths)

  val pipeline = new Pipeline(inputReaders,persistentData,stageProcessing,coreProcessing,resultWriter)

  pipeline.run()

}
