package com.tmobile.sit.ignite.hotspot.config

import java.sql.Timestamp

import com.tmobile.sit.common.config.GenericSettings

case class StageConfig(stage_folder: Option[String],
                       wlan_hotspot_filename: Option[String],
                       error_codes_filename: Option[String],
                       wlan_cdr_file: Option[String],
                       map_voucher_filename: Option[String],
                       orderDB_filename: Option[String],
                       exchange_rates_filename: Option[String],
                       city_data: Option[String],
                       wlan_voucher: Option[String],
                       login_errors: Option[String],
                       session_d: Option[String],
                       failed_transactions: Option[String],
                       orderDB_H: Option[String],
                       session_q: Option[String],
                       failed_logins: Option[String]
                      )

case class AppConfig(
                      processing_date: Option[Timestamp],
                      DES_encoder_path: Option[String],
                      wina_reports_day: Option[String]
                    )

case class InputConfig(
                        input_folder: Option[String],
                        MPS_filename: Option[String],
                        CDR_filename: Option[String],
                        exchange_rates_filename: Option[String],
                        failed_login_filename: Option[String]
                      )
case class OutputConfig(
                         output_folder: Option[String],
                         wina_report_tmd: Option[String],
                         wina_report: Option[String]
                       )

case class Settings (
                     inputConfig: InputConfig,
                     outputConfig: OutputConfig,
                     appConfig: AppConfig,
                     stageConfig: StageConfig
                   ) extends GenericSettings
{
  override def isAllDefined: Boolean = true
}
