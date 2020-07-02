package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.{CSVReader}
import com.tmobile.sit.ignite.rcseu.config.Setup
import com.tmobile.sit.ignite.rcseu.pipeline.{CoreLogicWithTransform, InputData, Pipeline, ResultPaths, ResultWriter, Stage}

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
    activity = new CSVReader(conf.settings.inputPath.get + "activity_*.gz", header = true, delimiter = "\t"),
    provision = new CSVReader(conf.settings.inputPath.get + "provision_*.gz", header = true, delimiter = "\t"),
    register_requests = new CSVReader(conf.settings.inputPath.get + "register_request*.gz", header = true, delimiter = "\t")
  )

  val stage = new Stage()

  val processingCore = new CoreLogicWithTransform()

  val resultPaths = ResultPaths(conf.settings.lookupPath.get, conf.settings.outputPath.get)
  val resultWriter = new ResultWriter(resultPaths)

  val pipeline = new Pipeline(inputReaders,stage,processingCore,resultWriter)

  pipeline.run()

}
