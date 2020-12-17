package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.RunConfig
import com.tmobile.sit.ignite.rcseu.data.{FileSchemas, InputData, PersistentData}
import com.tmobile.sit.ignite.rcseu.pipeline.{Configurator, Core, Helper, Pipeline, ResultWriter, Stage}
import org.apache.spark.sql.functions.{col, split}

object Application extends App with Logger {

  // First of all check arguments
  if (args.length != 3) {
    logger.error("Wrong arguments. Usage: ... <date:yyyy-mm-dd> <natco:mt|cg|st|cr|mk> <runFor:yearly|daily|update>")
    System.exit(0)
  }

  // Get the run variables based on input arguments
  val runVar = new RunConfig(args)

  logger.info(s"Date: ${runVar.date}, month:${runVar.month}, year:${runVar.year}, natco:${runVar.natco}, " +
    s"natcoNetwork: ${runVar.natcoNetwork}, runMode:${runVar.runMode} ")

  // Get settings and create spark session
  val settings = new Configurator().getSettings()
  implicit val sparkSession = getSparkSession(settings.appName.get)

  // Instantiate helper and resolve source file paths
  val h = new Helper()
  val sourceFilePath = h.resolvePath(settings)
  val activityFiles = h.resolveActivity(sourceFilePath)
  val fileMask = h.getArchiveFileMask()

  // Read sources
  val inputReaders = InputData(
    // Special treatment to resolve activity in case the runMode is 'update'
    activity = activityFiles,
    provision = new CSVReader(sourceFilePath + s"provision_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t").read(),
    register_requests = new CSVReader(sourceFilePath + s"register_requests_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t").read()
  )

  logger.info("Source files loaded")

  // read whole year only if doing yearly processing
  logger.info(s"Reading archive files for: ${fileMask}")

  val persistentData = PersistentData(
    oldUserAgents = new CSVReader(settings.lookupPath.get + "User_agents.csv", header = true, delimiter = "\t").read(),

    activity_archives = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\\t")
      .schema(FileSchemas.activitySchema)
      .csv(settings.archivePath.get + s"activity*${fileMask}*${runVar.natco}.csv*")
      //.withColumn("creation_date", split(col("creation_date"), "\\.").getItem(0))
      //.distinct()
    ,
    provision_archives = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(FileSchemas.provisionSchema)
      .csv(settings.archivePath.get + s"provision*${fileMask}*${runVar.natco}.csv*"),
    register_requests_archives = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\\t")
      .schema(FileSchemas.registerRequestsSchema)
      .csv(settings.archivePath.get + s"register_requests*${fileMask}*${runVar.natco}*.csv*")
  )

  persistentData.activity_archives.show(false)

  logger.info(s"Persistent files loaded for ${fileMask}")

  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultWriter = new ResultWriter(settings)

  logger.info("Running pipeline")

  val pipeline = new Pipeline(inputReaders, persistentData, stageProcessing, coreProcessing, resultWriter)

  pipeline.run()

}
