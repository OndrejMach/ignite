package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.{CSVReader, Reader}
import com.tmobile.sit.ignite.rcseu.config.Setup
import com.tmobile.sit.ignite.rcseu.pipeline.{CoreLogicWithTransform, InputData, Pipeline, Stage}

case class Inputs(input1: Reader, input2: Reader, input3: Reader)


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
    provision = new CSVReader(conf.settings.inputPath.get + "provision_*.csv", header = true, delimiter = "\t"),
    register_requests = new CSVReader(conf.settings.inputPath.get + "register_request*.csv", header = true, delimiter = "\t")
  )

  val stage = new Stage()

  val processingCore = new CoreLogicWithTransform()

  val pipeline = new Pipeline(inputReaders,stage,processingCore, conf.settings)

  pipeline.run()

}
