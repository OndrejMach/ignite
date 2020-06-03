package com.tmobile.sit.ignite.hotspot

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.config.Setup
import com.tmobile.sit.ignite.hotspot.processors._


object Application extends Logger {

  implicit val sparkSession = getSparkSession()

  implicit val settings = new Setup().settings

  //settings.printAllFields()

  def main(args: Array[String]): Unit = {

    settings.printAllFields()

    val processor = args(0) match {
      case "exchangeRates" => new ExchangeRatesProcessor()
      case "input" => new InputFilesProcessor()
      case "stage" => new StageFilesProcessor()
      case "wina_reports" => new WinaReportsProcessor()
      case "output" => new OutputsProcessor()
      case _ => new HelperProcessor()
    }
    processor.process()

  }
}
