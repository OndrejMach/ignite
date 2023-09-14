package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.{CSVReader, ParquetReader}
import com.tmobile.sit.ignite.common.common.writers.ParquetWriter
import com.tmobile.sit.ignite.rcseu.Application.{fileMask, runVar}
import com.tmobile.sit.ignite.rcseu.config.{Settings, Setup}
import com.tmobile.sit.ignite.rcseu.data.FileSchemas
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Config extends Logger{
  def getSettings():Settings
}
trait Help extends Logger{
  def resolvePath(settings:Settings):String
  def resolveInputPath(settings:Settings):String
  def getArchiveFileMask():String
  def resolveActivity(sourceFilePath: String):DataFrame
  def resolveCSVFiles(inputFilePath: String, sourceFilePath: String): Unit
}

class Helper() (implicit sparkSession: SparkSession) extends Help {

  override def getArchiveFileMask():String = {
    // if yearly reprocessing or update on 31st of January
    if(runVar.runMode.equals("yearly") ||
      (runVar.runMode.equals("update") && runVar.date.endsWith("-12-31"))) {
      logger.info("Processing yearly archive data")
      runVar.year
    } else {
      logger.info("Processing daily and monthly archive data")
      runVar.month
    }
  }

  override def resolveActivity(sourceFilePath: String): DataFrame = {
    if (runVar.runMode.equals("update")) {
      logger.info("runMode: update")
      logger.info(s"Reading activity data for ${runVar.date} and ${runVar.tomorrowDate}")
      sparkSession.read
        .schema(FileSchemas.activitySchema)
        .option("mergeSchema", "True")
        .parquet(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.parquet*",
          sourceFilePath + s"activity_${runVar.tomorrowDate}*${runVar.natco}.parquet*")
    }
    else {
      logger.info(s"runMode: ${runVar.runMode}, reading daily activity")
      new ParquetReader(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.parquet*",
        schema = Some(FileSchemas.activitySchema)).read()
    }
  }

  def resolveCSVFiles(inputFilePath: String, sourceFilePath: String): Unit = {
    logger.info(s"Reading activity csv data for ${runVar.date}")
    val activityData = new CSVReader(inputFilePath + s"activity_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read()

    logger.info(s"Writing activity parquet data for ${runVar.date}")
    ParquetWriter(activityData, sourceFilePath + s"activity_${runVar.date}_${runVar.natco}.parquet").writeParquetData()

    logger.info(s"Reading provision csv data for ${runVar.date}")
    val provisionData = new CSVReader(inputFilePath + s"provision_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t").read()

    logger.info(s"Writing provision parquet data for ${runVar.date}")
    ParquetWriter(provisionData, sourceFilePath + s"provision_${runVar.date}_${runVar.natco}.parquet").writeParquetData()

    logger.info(s"Reading register requests csv data for ${runVar.date}")
    val registerRequestsData = new CSVReader(inputFilePath + s"register_requests_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t").read()

    logger.info(s"Writing register requests parquet data for ${runVar.date}")
    ParquetWriter(registerRequestsData, sourceFilePath + s"register_requests_${runVar.date}_${runVar.natco}.parquet").writeParquetData()
  }


  override def resolvePath(settings:Settings):String = {
    // always reading daily data from the archive folder
    settings.archivePath.get
  }

  override def resolveInputPath(settings: Settings): String = {
    // always reading daily data from the archive folder
    settings.inputPath.get
  }
}

class Configurator() extends Config {
  override def getSettings(): Settings = {
    val configFile = if(System.getProperty("os.name").startsWith("Windows")) {
      logger.info(s"Detected development configuration (${System.getProperty("os.name")})")
      "rcs-eu.windows.conf"
    } else if (System.getProperty("os.name").startsWith("Mac OS")){
      "rcs-eu.osx.conf"
    }
    else {
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

    conf.settings
  }
}