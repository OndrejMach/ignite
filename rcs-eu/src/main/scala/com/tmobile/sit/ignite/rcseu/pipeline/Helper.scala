package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.{runVar}
import com.tmobile.sit.ignite.rcseu.config.{Settings, Setup}
import org.apache.spark.sql.SparkSession

trait Config extends Logger{
  def getSettings():Settings
}
trait Help extends Logger{
  def resolvePath(settings:Settings):String
}

class Helper() (implicit sparkSession: SparkSession) extends Help {

  override def resolvePath(settings:Settings):String = {
    // if historic, file is found in the archive

    val path = if(runVar.isHistoric) { settings.archivePath.get} else { settings.inputPath.get }

    /*
    // expected file like /data/sit/rcseu/input|archive/activity_2020-01-01.csv_mt.csv.gz
    val expectedFile = s"${path}${fileName}_${run.date}.csv_${run.natco}.csv.gz"
    // if file doesn't exist, use dummy one
    val dummyFile = s"${settings.lookupPath.get}empty_${fileName}.csv"

    //check for file and if historic
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(expectedFile))

    var result = new String

    if(fileExists) {
      logger.info(s"File $expectedFile exists: $fileExists")
      result = expectedFile
    } else { //file doesn't exist
      logger.warn(s"File $expectedFile exists: $fileExists")
      if(run.isHistoric) { // If during a historic reprocessing run, replace with dummy file and move on
        logger.warn(s"Using dummy file: $dummyFile")
        result = dummyFile
      }
      else {
        logger.error(s"File missing during non-historic run: $expectedFile")
        System.exit(1)
      }
    }
    // Return correct result
    result
    */

    path
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