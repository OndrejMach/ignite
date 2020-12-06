package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.Application.{fileMask, runVar}
import com.tmobile.sit.ignite.rcseu.config.{Settings, Setup}
import com.tmobile.sit.ignite.rcseu.data.FileSchemas
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Config extends Logger{
  def getSettings():Settings
}
trait Help extends Logger{
  def resolvePath(settings:Settings):String
}

class Helper() (implicit sparkSession: SparkSession) extends Help {

  def resolveActivity(sourceFilePath: String):DataFrame = {
    if(runVar.runMode.equals("update")) {
      logger.info("runMode: update")
      logger.info(s"Reading activity data for ${runVar.date} and ${runVar.tomorrowDate}")
      sparkSession.read
        .option("header", "true")
        .option("delimiter","\\t")
        .schema(FileSchemas.activitySchema)
        .csv(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.csv.gz",
             sourceFilePath + s"activity_${runVar.tomorrowDate}*${runVar.natco}.csv.gz")}
    else {
      logger.info(s"runMode: ${runVar.runMode}, reading daily activity")
      new CSVReader(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.csv.gz",
        schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read()
    }
  }


  override def resolvePath(settings:Settings):String = {
    // always reading daily data from the archive folder
    settings.archivePath.get
  }
}

class Configurator() extends Config {
  override def getSettings(): Settings = {
    val configFile = if(System.getProperty("os.name").startsWith("Windows")) {
      logger.info(s"Detected development configuration (${System.getProperty("os.name")})")
      "rcs-eu.windows.conf"
    } else {
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