package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.{CSVReader, ParquetReader}
import com.tmobile.sit.ignite.common.common.writers.ParquetWriter
import com.tmobile.sit.ignite.rcseu.Application.{fileMask, runVar}
import com.tmobile.sit.ignite.rcseu.config.{Settings, Setup}
import com.tmobile.sit.ignite.rcseu.data.FileSchemas
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Config extends Logger{
  def getSettings():Settings
}
trait Help extends Logger{
  def resolvePath(settings:Settings):String
  def resolveInputPath(settings:Settings):String
  def getArchiveFileMask():String
  def resolveActivity(sourceFilePath: String):DataFrame
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
        .parquet(sourceFilePath + s"activity/natco=${runVar.natco}/year=${runVar.year}/month=${runVar.monthNum}/day=${runVar.dayNum}",
          sourceFilePath + s"activity/natco=${runVar.natco}/year=${runVar.year}/month=${runVar.monthNum}/day=${runVar.tomorrowDay}")
//        .parquet(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.parquet*",
//          sourceFilePath + s"activity_${runVar.tomorrowDate}*${runVar.natco}.parquet*")
    }
    else {
      logger.info(s"runMode: ${runVar.runMode}, reading daily activity")
      new ParquetReader(sourceFilePath + s"activity/natco=${runVar.natco}/year=${runVar.year}/month=${runVar.monthNum}/day=${runVar.dayNum}",
        sourceFilePath + s"activity/",
//      new ParquetReader(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.parquet*",
        schema = Some(FileSchemas.activitySchema)).read()
    }
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