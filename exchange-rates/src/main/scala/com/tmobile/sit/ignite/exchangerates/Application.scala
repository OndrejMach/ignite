package com.tmobile.sit.ignite.exchangerates

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.exchangerates.config.Setup
import com.tmobile.sit.ignite.exchangerates.processing.ExchangeRatesProcessor

/**
 * Application object used for processing launch.
 * Based on the commandline parameters it executes either input, stage, output or wina reports calculation.
 * Generally the order should be exchangeRates & input -> stage -> output -> wina_reports.
 */
object Application extends Logger {
  //settings.printAllFields()

  def main(args: Array[String]): Unit = {

    implicit val settings = new Setup().settings

    implicit val sparkSession = getSparkSession(settings)

    settings.printAllFields()

    val processor =  new ExchangeRatesProcessor()

    processor.process()

  }
}
