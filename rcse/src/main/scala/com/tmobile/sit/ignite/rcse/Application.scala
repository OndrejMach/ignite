package com.tmobile.sit.ignite.rcse

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Setup
import com.tmobile.sit.ignite.rcse.stages._

/**
 * Main object - rcse shall be running in the followint regimes (order is important for the daily run):
 * 1) terminalD
 * 2) stage
 * 3) aggregates
 * 4) output
 */
object Application extends App with Logger{

  implicit val settings = new Setup().settings
  implicit val sparkSession = getSparkSession(settings.app.master, settings.app.dynamicAllocation)

  sparkSession.sql("set spark.sql.shuffle.partitions=10")

  settings.printAllFields()

  if (!settings.isAllDefined) {
    logger.error("Parameters are not defined")
    System.exit(1)
  }

  val regime = if (args.length > 0 ) args(0) else "helper"
  logger.info(s"Running RCSE processing in the ${regime} regime")
  val executor = regime match {
    case "terminalD" => new TerminalD()
    case "events" => new Events()
    case "output" => new Outputs()
    case "aggregates" => new Aggregates()
    case "conf" => new Conf()
    case "activeUsers" => new ActiveUsers()
    case _ => new Helper()
  }

  executor.runProcessing()
  sparkSession.stop()


}