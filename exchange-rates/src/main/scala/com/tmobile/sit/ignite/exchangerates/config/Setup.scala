package com.tmobile.sit.ignite.exchangerates.config

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.config.ServiceConfig

/**
 * this class helps to read parameters from the configuration file and stores them to the wrapper case classes. Path to configuration file may be provided as a parameter.
 * @param configFile
 */


class Setup(configFile: String = "exchange_rates.conf") extends Logger {

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
        exchange_rates_filename = serviceConf.getString("config.input.exchange_rates_filename")
      ),
      stageConfig = StageConfig(
        stage_folder = serviceConf.getString("config.stage.stage_folder"),
        exchange_rates_filename = serviceConf.getString("config.stage.exchange_rates_filename")
       //login_errors
      ),
      appConfig = AppConfig(
        input_date = Some(getProcessingDate(serviceConf.getString("config.input_date"))),
        application_name = serviceConf.getString("config.application_name"),
        master = serviceConf.getString("config.master")
      )
    )
  }
}
