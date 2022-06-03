package com.tmobile.sit.ignite.rcseu.config

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.config.{GenericSettings, ServiceConfig}

case class Settings(inputPath: Option[String]
                    , outputPath: Option[String]
                    , lookupPath: Option[String]
                    , archivePath: Option[String]
                    , appName: Option[String]
                   ) extends GenericSettings {

  def isAllDefined: Boolean = {
    this.inputPath.isDefined && this.inputPath.get.nonEmpty &&
      this.lookupPath.isDefined && this.lookupPath.get.nonEmpty &&
      this.outputPath.isDefined && this.outputPath.get.nonEmpty &&
      this.archivePath.isDefined && this.archivePath.get.nonEmpty &&
      this.appName.isDefined && this.appName.get.nonEmpty
  }
}

object Settings extends Logger {
  def getConfigFile(): String = {
    if (System.getProperty("os.name").startsWith("Windows")) {
      logger.info(s"Detected development configuration (${System.getProperty("os.name")})")
      "rcs-eu.windows.conf"
    } else if (System.getProperty("os.name").startsWith("Mac OS")){
      "rcs-eu.osx.conf"
    }
    else {
      logger.info(s"Detected production configuration (${System.getProperty("os.name")})")
      "rcs-eu.linux.conf"
    }
  }

  def loadFile(configFile: String = "rcs-eu.linux.conf"): Settings = {
    logger.info("Configuration setup for " + configFile)
    val serviceConf = new ServiceConfig(Some(configFile))

    val settings = Settings(
      appName = Option(serviceConf.envOrElseConfig("configuration.appName.value"))
      , inputPath = Option(serviceConf.envOrElseConfig("configuration.inputPath.value"))
      , lookupPath = Option(serviceConf.envOrElseConfig("configuration.lookupPath.value"))
      , outputPath = Option(serviceConf.envOrElseConfig("configuration.outputPath.value"))
      , archivePath = Option(serviceConf.envOrElseConfig("configuration.archivePath.value")
      ))

    if (!settings.isAllDefined) {
      logger.error("Application not properly configured!!")
      settings.printMissingFields()
      System.exit(1)
    }

    settings.printAllFields()
    settings
  }
}