package com.tmobile.sit.ignite.rcse.config

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.config.GenericSettings

/**
 * Wrapper classes for holding application parameters and their values. Also implements couple of methods for printing and validating of the
 * configuration.
 */

abstract class RCSEConfig extends Logger {
  def printAllFields(): Unit = {
    logger.info("PARAMETER VALUES: ")
    val fields = this.getClass.getDeclaredFields
    fields.foreach(_.setAccessible(true))
    fields.foreach(f => logger.info(s"${f.getName} : ${f.get(this)}"))
  }

  def isAllDefined: Boolean = {
    val fields = this.getClass.getDeclaredFields
    fields.foreach(_.setAccessible(true))
    fields.map(_.get(this).asInstanceOf[AnyVal]).map(f => f.toString.length > 0).reduce(_ && _)
  }
}

case class StageFilesConfig(
                             clientPath: String,
                             terminalSWPath: String,
                             imsisEncodedPath: String,
                             msisdnsEncodedPath: String,
                             terminalPath: String,
                             tacPath: String,
                             regDerEvents: String,
                             activeUsers: String,
                             confFile: String,
                             initUser: String,
                             initConf: String,
                             dmEventsFile: String,
                             uauFile: String
                           ) extends RCSEConfig

case class OutputConfig(
                         client: String,
                         terminal: String,
                         terminalSW: String,
                         activeUsers: String,
                         uauFile: String,
                         initConf: String,
                         initUser: String
                       ) extends RCSEConfig

case class AppConfig(
                      processingDate: Date,
                      inputFilesPath: String,
                      maxDate: Date,
                      master: String
                    ) extends RCSEConfig


case class Settings(
                     app: AppConfig,
                     stage: StageFilesConfig,
                     output: OutputConfig
                   ) extends GenericSettings
{
  override def isAllDefined: Boolean = app.isAllDefined && stage.isAllDefined && output.isAllDefined

  override def printAllFields(): Unit = {
    logger.info("APP CONFIG:")
    app.printAllFields()
    logger.info("STAGE CONFIG:")
    stage.printAllFields()
    logger.info("OUTPUT CONFIG:")
    output.printAllFields()
  }
}

