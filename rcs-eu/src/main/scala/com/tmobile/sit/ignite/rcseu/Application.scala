package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Setup}
import com.tmobile.sit.ignite.rcseu.data.{FileSchemas, InputData, PersistentData}
import com.tmobile.sit.ignite.rcseu.pipeline.{Configurator, Core, Helper, Pipeline, ResultWriter, Stage}

object Application extends App with Logger {

  //TODO: implement better flags like run-daily, run-monthly, run-yearly, run-all, run-debug
  if(args.length != 3) {
    logger.error("No arguments specified. Usage: ... <date:yyyy-mm-dd> <natco:cc> <isHistoric:bool>")
    System.exit(0)
  }

  // DEBUG variable to be used in other methods
  val debug = false;
  val runVar = new RunConfig(args)

  logger.info(s"Date: ${runVar.date}, month:${runVar.month}, year:${runVar.year}, natco: ${runVar.natco}, " +
    s"natcoNetwork: ${runVar.natcoNetwork}, isHistoric: ${runVar.isHistoric}")

  val settings = new Configurator().getSettings()
  implicit val sparkSession = getSparkSession(settings.appName.get)

  val h = new Helper()
  val sourceFilePath = h.resolvePath(settings)

  val inputReaders = InputData(
    activity = new CSVReader(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.csv.gz",
      schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read(),
    provision = new CSVReader(sourceFilePath + s"provision_${runVar.date}*${runVar.natco}.csv.gz",
      schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t").read(),
    register_requests = new CSVReader(sourceFilePath + s"register_requests_${runVar.date}*${runVar.natco}.csv.gz",
      schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t").read()
  )
  logger.info("Source files loaded")

  val persistentData = PersistentData(
    oldUserAgents = new CSVReader(settings.lookupPath.get + "User_agents.csv", header = true, delimiter = "\t").read(),
    //TODO: see if this works with CSVReader class class for consistency
    activity_archives = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter","\\t")
      .schema(FileSchemas.activitySchema)
      .load(settings.archivePath.get + s"activity*${runVar.year}*${runVar.natco}.csv*"),
    provision_archives = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter","\\t")
      .schema(FileSchemas.provisionSchema)
      .load(settings.archivePath.get + s"provision*${runVar.year}*${runVar.natco}.csv*"),
    register_requests_archives = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter","\\t")
      .schema(FileSchemas.registerRequestsSchema)
      .load(settings.archivePath.get + s"register_requests*${runVar.year}*${runVar.natco}*.csv*")
  )

  logger.info("Persistent files loaded")

  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultWriter = new ResultWriter(settings)

  logger.info("Running pipeline")

  val pipeline = new Pipeline(inputReaders,persistentData,stageProcessing,coreProcessing,resultWriter)

  pipeline.run()

}
