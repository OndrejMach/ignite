package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.ignite.common.common.Logger
//import com.tmobile.sit.ignite.common.common.readers.CSVReader
import com.tmobile.sit.ignite.common.common.readers.ParquetReader
import com.tmobile.sit.ignite.rcseu.config.RunConfig
import com.tmobile.sit.ignite.rcseu.data.{FileSchemas, InputData, PersistentData}
import com.tmobile.sit.ignite.rcseu.pipeline._
import org.apache.spark.sql.functions.broadcast

object ParquetApplication extends App with Logger {

  // First of all check arguments
  if (args.length != 3) {
    logger.error("Wrong arguments. Usage: ... <date:yyyy-mm-dd> <natco:mt|cg|st|cr|mk> <runFor:yearly|daily|update>")
    System.exit(0)
  }

  // Get the run variables based on input arguments
  val runVar = new RunConfig(args)
  println(runVar)
  logger.info(s"Date: ${runVar.date}, month:${runVar.month}, year:${runVar.year}, natco:${runVar.natco}, " +
    s"natcoNetwork: ${runVar.natcoNetwork}, runMode:${runVar.runMode} ")

  // Get settings and create spark session
  val settings = new Configurator().getSettings()
  implicit val sparkSession = getSparkSession(settings.appName.get)

  // Instantiate helper and resolve source file paths
  val h = new ParquetHelper()
  val sourceFilePath = h.resolvePath(settings)
  val activityFiles = h.resolveActivity(sourceFilePath)
  val fileMask = h.getArchiveFileMask()

  println(activityFiles.count())

  // Read sources
//  schema = Some(FileSchemas.provisionSchema)
//  schema = Some(FileSchemas.registerRequestsSchema)
  val inputReaders = InputData(
    // Special treatment to resolve activity in case the runMode is 'update'
    activity = activityFiles,
    provision = new ParquetReader(sourceFilePath + s"provision_${runVar.date}*${runVar.natco}.parquet*",
      schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t").read(),
    register_requests = new ParquetReader(sourceFilePath + s"register_requests_${runVar.date}*${runVar.natco}.parquet*",
      schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t").read()
  )

  logger.info("Input files loaded")

  // read whole year only if doing yearly processing
  logger.info(s"Reading archive files for: ${fileMask}")

  val persistentData = PersistentData(
    oldUserAgents = broadcast(new ParquetReader(settings.lookupPath.get + "User_agents.parquet", header = true, delimiter = "\t").read()),

    activity_archives = sparkSession.read
      .schema(FileSchemas.activitySchema)
      .option("mergeSchema", "True")
      .parquet(settings.archivePath.get + s"activity*${fileMask}*${runVar.natco}.parquet*")
      //.repartition(20)
      //.withColumn("creation_date", split(col("creation_date"), "\\.").getItem(0))
      //.distinct()
    ,
    provision_archives = sparkSession.read
      .schema(FileSchemas.provisionSchema)
      .option("mergeSchema", "True")
      .parquet(settings.archivePath.get + s"provision*${fileMask}*${runVar.natco}.parquet*")
      //.repartition(20)
    ,
    register_requests_archives = sparkSession.read
      .schema(FileSchemas.registerRequestsSchema)
      .option("mergeSchema", "True")
      .parquet(settings.archivePath.get + s"register_requests*${fileMask}*${runVar.natco}*.parquet*")
      //.repartition(20)
  )

  logger.info(s"Archive files loaded for file_mask=[${fileMask}*]")
  //persistentData.activity_archives.show(false)
  val stageProcessing = new Stage()

  val coreProcessing = new Core()

  val resultWriter = new ResultWriter(settings)

  logger.info("Running pipeline")

  val pipeline = new Pipeline(inputReaders, persistentData, stageProcessing, coreProcessing, resultWriter)

  pipeline.run()

}
