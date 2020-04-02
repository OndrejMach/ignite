package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.config.Setup

object Application extends Logger{
  def main(args: Array[String]): Unit = {
    logger.info(s"Reading configuation files")
    val setup = new Setup()
    logger.info(s"Configuration parameters check")
    if (!setup.settings.isAllDefined){
      logger.error("Application parameters not properly defined")
      setup.settings.printMissingFields()
    }
    logger.info("Configuration parameters OK")
    setup.settings.printAllFields()

    logger.info("Getting SparkSession")
    implicit val sparkSession = getSparkSession()


    val processor: Processor = args(0) match {
      case "monthly" => new MonthlyReport(setup.settings)
      case _ => new DailyCalculation(setup.settings)
    }

    processor.executeCalculation()
  }
}
