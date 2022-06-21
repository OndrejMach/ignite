package com.tmobile.sit.ignite.exchangerates.config

import java.sql.Timestamp

import com.tmobile.sit.ignite.common.common.config.GenericSettings

/**
 * Wrapper classes for configuration parameters.
 * The structure basically follows the schema of the application configuration file. Classes also implement couple of supporting methods helping with validity checks or printing the values.
 */


abstract class FilesConfig extends GenericSettings {
  def isAllDefined: Boolean = {
    val fields = this.getClass.getDeclaredFields
    fields.foreach(_.setAccessible(true))
    fields.map(_.get(this).asInstanceOf[Option[String]]).map(f => f.isDefined && !f.isEmpty).reduce(_ && _)
  }
}


case class StageConfig(stage_folder: Option[String],
                       exchange_rates_filename: Option[String]
                      ) extends FilesConfig

case class AppConfig(
                      input_date: Option[Timestamp],
                      application_name: Option[String],
                      master: Option[String]
                    ) extends GenericSettings {
  override def isAllDefined = {
    master.isDefined  && input_date.isDefined
  }
}

case class InputConfig(
                        input_folder: Option[String],
                        exchange_rates_filename: Option[String]
                      ) extends FilesConfig

case class Settings(
                     inputConfig: InputConfig,
                     appConfig: AppConfig,
                     stageConfig: StageConfig
                   ) extends GenericSettings {
  override def isAllDefined: Boolean = true

  override def printAllFields(): Unit = {
    logger.info(s"${Console.RED}INPUT PARAMETERS:${Console.RESET}")
    inputConfig.printAllFields()
    logger.info(s"${Console.RED}STAGE DATA PARAMETERS:${Console.RESET}")
    stageConfig.printAllFields()
    logger.info(s"${Console.RED}APPLICATION PARAMETERS:${Console.RESET}")
    appConfig.printAllFields()
  }

}
