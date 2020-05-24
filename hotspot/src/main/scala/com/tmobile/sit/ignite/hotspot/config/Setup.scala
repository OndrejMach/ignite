package com.tmobile.sit.ignite.hotspot.config

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.config.ServiceConfig

class Setup(configFile: String = "hotspot.conf") extends Logger {

  private def getProcessingDate(s: Option[String]): Timestamp = {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    try {
      val date = LocalDate.parse(s.get, formatter)
      Timestamp.valueOf(LocalDateTime.of(date.getYear, date.getMonth, date.getDayOfMonth, 0, 0, 0))
    } catch {
      case x: Exception => {
        logger.warn(s"Processing date not correctly defined ${s}, using TODAY's date (${x.getMessage})")
        val date = LocalDate.now()
        Timestamp.valueOf(LocalDateTime.of(date.getYear, date.getMonth, date.getDayOfMonth, 0, 0, 0))
      }
    }
  }


  val settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      inputConfig = InputConfig(
        input_folder = serviceConf.getString("config.input.input_folder"),
        MPS_filename = serviceConf.getString("config.input.MPS_filename"),
        CDR_filename = serviceConf.getString("config.input.CDR_filename"),
        exchange_rates_filename = serviceConf.getString("config.input.exchange_rates_filename"),
        failed_login_filename = serviceConf.getString("config.input.failed_login_filename")//failed_login_filename
      ),
      stageConfig = StageConfig(
        stage_folder = serviceConf.getString("config.stage.stage_folder"),
        wlan_hotspot_filename = serviceConf.getString("config.stage.wlan_hotspot_filename"),
        error_codes_filename = serviceConf.getString("config.stage.error_codes_filename"),
        wlan_cdr_file = serviceConf.getString("config.stage.wlan_cdr_file"),
        map_voucher_filename = serviceConf.getString("config.stage.map_voucher_filename"),
        orderDB_filename = serviceConf.getString("config.stage.orderDB_filename"),
        exchange_rates_filename = serviceConf.getString("config.stage.exchange_rates_filename"),
        city_data = serviceConf.getString("config.stage.city_data"),
        wlan_voucher = serviceConf.getString("config.stage.wlan_voucher"),
        login_errors = serviceConf.getString("config.stage.login_errors"),
        session_d  =serviceConf.getString("config.stage.session_d") ,
        failed_transactions = serviceConf.getString("config.stage.failed_transactions"),
        orderDB_H = serviceConf.getString("config.stage.orderDB_H"),
        session_q = serviceConf.getString("config.stage.session_q"),
        failed_logins = serviceConf.getString("config.stage.failed_logins")//login_errors
      ),
      outputConfig = OutputConfig(
        output_folder = serviceConf.getString("config.output.output_folder"),
        wina_report = serviceConf.getString("config.output.wina_report"),
        wina_report_tmd = serviceConf.getString("config.output.wina_report_tmd")
      ),
      appConfig = AppConfig(
        processing_date = Some(getProcessingDate(serviceConf.getString("config.processing_date"))),
        DES_encoder_path = serviceConf.getString("config.3DES_encoder_path"), //3DES_encoder_path
        wina_reports_day = serviceConf.getString("config.wina_reports_day")
      )
    )
  }
}
