package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.Setup
import com.tmobile.sit.ignite.rcseu.data.{InputData, PersistentData, ResultPaths}
import com.tmobile.sit.ignite.rcseu.pipeline.{Core, Pipeline, ResultWriter, Stage}

object Application extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()

  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  val inputReaders = InputData(
    activity = new CSVReader(conf.settings.inputPath.get + "activity_*.csv", header = true, delimiter = "\t"),
    provision = new CSVReader(conf.settings.inputPath.get + "provision_*.gz", header = true, delimiter = "\t"),
    register_requests = new CSVReader(conf.settings.inputPath.get + "register_request*.gz", header = true, delimiter = "\t")
  )

  val persistentData = PersistentData(
    oldUserAgents = new CSVReader(conf.settings.outputPath.get + "UserAgents.csv", header = true, delimiter = ";").read()
  )

  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultPaths = ResultPaths(conf.settings.lookupPath.get, conf.settings.outputPath.get)
  val resultWriter = new ResultWriter(resultPaths)

  val pipeline = new Pipeline(inputReaders,persistentData,stageProcessing,coreProcessing,resultWriter)

  pipeline.run()

}
