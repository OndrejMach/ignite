package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.RCSEUParquetReader
import com.tmobile.sit.ignite.rcseu.config.RunConfig
import com.tmobile.sit.ignite.rcseu.data.{FileSchemas, InputData, PersistentData}
import com.tmobile.sit.ignite.rcseu.pipeline.{Configurator, Core, Helper, Pipeline, ResultWriter, Stage}

object Application extends App with Logger {

  // First of all check arguments
  if (args.length != 3) {
    logger.error("Wrong arguments. Usage: ... <date:yyyy-mm-dd> <natco:mt|cg|st|cr|mk> <runFor:yearly|daily|update>")
    System.exit(0)
  }

  // Get the run variables based on input arguments
  val runVar = new RunConfig(args)

  logger.info(s"Date: ${runVar.date}, month:${runVar.monthNum}, year:${runVar.year}, natco:${runVar.natco}, " +
    s"natcoNetwork: ${runVar.natcoNetwork}, runMode:${runVar.runMode} ")

  // Get settings and create spark session
  val settings = new Configurator().getSettings()
  implicit val sparkSession = getSparkSession(settings.appName.get)

  // Instantiate helper and resolve source file paths
  val h = new Helper()
  val inputFilePath = h.resolveInputPath(settings)
  val sourceFilePath = h.resolvePath(settings)
  val activityFiles = h.resolveActivity(sourceFilePath)
  val fileMask = h.getArchiveFileMask()

  // Read sources
  val inputReaders = InputData(
    // Special treatment to resolve activity in case the runMode is 'update'
    activity = activityFiles,
    provision = new RCSEUParquetReader(sourceFilePath + s"provision/natco=${runVar.natco}/year=${runVar.year}/month=${runVar.monthNum}/day=${runVar.dayNum}",
      sourceFilePath + s"provision/",
      addFileDate = true
    ).read(),
    register_requests = new RCSEUParquetReader(sourceFilePath + s"register_requests/natco=${runVar.natco}/year=${runVar.year}/month=${runVar.monthNum}/day=${runVar.dayNum}",
      sourceFilePath + s"register_requests/",
      addFileDate = true
    ).read()
  )

  logger.info("Input files loaded")

  // read whole year only if doing yearly processing

  var filePath = ""

  if (runVar.runMode.equals("yearly") ||
    (runVar.runMode.equals("update") && runVar.date.endsWith("-12-31"))) {
    filePath = s"/natco=${runVar.natco}/year=${runVar.year}/"
  }
  else {
    filePath = s"/natco=${runVar.natco}/year=${runVar.year}/month=${runVar.monthNum}/"
  }

  logger.info(s"Reading archive files for: ${fileMask}")

  val persistentData = PersistentData(
    oldUserAgents = new RCSEUParquetReader(settings.lookupPath.get + "User_agents.parquet", settings.lookupPath.get + "User_agents.parquet").read(),
    activity_archives = new RCSEUParquetReader(settings.archivePath.get + "activity" + filePath, settings.archivePath.get + s"activity/", addFileDate = true).read(),
    provision_archives = new RCSEUParquetReader(settings.archivePath.get + "provision" + filePath, settings.archivePath.get + s"provision/", addFileDate = true).read(),
    register_requests_archives = new RCSEUParquetReader(settings.archivePath.get + "register_requests" + filePath, settings.archivePath.get + s"register_requests", addFileDate = true).read()
  )

  logger.info(s"Archive files loaded for file_mask=[${filePath}*]")

  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultWriter = new ResultWriter(settings)

  logger.info("Running pipeline")

  val pipeline = new Pipeline(inputReaders, persistentData, stageProcessing, coreProcessing, resultWriter)

  pipeline.run()

}
