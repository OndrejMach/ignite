package com.tmobile.sit.ignite.hotspot.config

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
                       wlan_hotspot_filename: Option[String],
                       error_codes_filename: Option[String],
                       wlan_cdr_file: Option[String],
                       map_voucher_filename: Option[String],
                       orderDB_filename: Option[String],
                       city_data: Option[String],
                       wlan_voucher: Option[String],
                       login_errors: Option[String],
                       session_d: Option[String],
                       exchange_rates_filename: Option[String],
                       failed_transactions: Option[String],
                       orderDB_H: Option[String],
                       session_q: Option[String],
                       failed_logins: Option[String],
                       country: Option[String],
                       failed_logins_input: Option[String]
                      ) extends FilesConfig

case class AppConfig(
                      processing_date: Option[Timestamp],
                      DES_encoder_path: Option[String],
                      wina_reports_day: Option[String],
                      input_date: Option[Timestamp],
                      application_name: Option[String],
                      master: Option[String]
                    ) extends GenericSettings {
  override def isAllDefined = {
    processing_date.isDefined && DES_encoder_path.isDefined && wina_reports_day.isDefined && input_date.isDefined
  }
}

case class InputConfig(
                        input_folder: Option[String],
                        MPS_filename: Option[String],
                        CDR_filename: Option[String],
                        failed_login_filename: Option[String]
                      ) extends FilesConfig

case class OutputConfig(
                         output_folder: Option[String],
                         wina_report_tmd: Option[String],
                         wina_report: Option[String],
                         sessio_d: Option[String],
                         orderDB_h: Option[String],
                         session_q: Option[String],
                         error_code: Option[String],
                         hotspot_ta_d: Option[String],
                         voucher: Option[String],
                         city: Option[String],
                         country: Option[String],
                         failed_trans: Option[String],
                         failed_login: Option[String],
                         login_error: Option[String],
                         hotspot_vi_d: Option[String]
                       ) extends FilesConfig

case class Settings(
                     inputConfig: InputConfig,
                     outputConfig: OutputConfig,
                     appConfig: AppConfig,
                     stageConfig: StageConfig
                   ) extends GenericSettings {
  override def isAllDefined: Boolean = true

  override def printAllFields(): Unit = {
    logger.info(s"${Console.RED}INPUT PARAMETERS:${Console.RESET}")
    inputConfig.printAllFields()
    logger.info(s"${Console.RED}OUTPUT PARAMETERS:${Console.RESET}")
    outputConfig.printAllFields()
    logger.info(s"${Console.RED}REFERENCE DATA PARAMETERS:${Console.RESET}")
    stageConfig.printAllFields()
    logger.info(s"${Console.RED}APPLICATION PARAMETERS:${Console.RESET}")
    appConfig.printAllFields()
  }

}
