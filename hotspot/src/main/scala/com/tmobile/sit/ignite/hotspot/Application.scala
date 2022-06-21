package com.tmobile.sit.ignite.hotspot

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.hotspot.config.Setup
import com.tmobile.sit.ignite.hotspot.processors._

/**
 * Application object used for processing launch.
 * Based on the commandline parameters it executes either input, stage, output or wina reports calculation.
 * Generally the order should be exchangeRates & input -> stage -> output -> wina_reports.
 */
object Application extends Logger {
  implicit val settings = new Setup().settings

  implicit val sparkSession = getSparkSession(settings)

  def main(args: Array[String]): Unit = {

    settings.printAllFields()

    val processor = args(0) match {
      case "input" => new InputFilesProcessor()
      case "stage" => new StageFilesProcessor()
      case "wina_reports" => new WinaReportsProcessor()
      case "output" => new OutputsProcessor()
      case _ => new HelperProcessor()
    }
    processor.process()

  }
}
