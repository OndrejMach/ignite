package com.tmobile.sit.ignite.rcse

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.{Settings, Setup}
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsData
import com.tmobile.sit.ignite.rcse.processors.{ActiveUsersToStage, AggregateUAU, ConfToStage, EventsToStage, InitConfAggregates, InitUserAggregates, TerminalDProcessor}
import com.tmobile.sit.ignite.rcse.stages.{Aggregates, Helper, Outputs, Stage, TerminalD}

object Application extends App with Logger{
  implicit val sparkSession = getSparkSession()

  implicit val settings = new Setup().settings

  settings.printAllFields()

  if (!settings.isAllDefined) {
    logger.error("Parameters are not defined")
    System.exit(1)
  }

  val regime = if (args.length > 0 ) args(0) else "helper"
  logger.info(s"Running RCSE processing in the ${regime} regime")
  val executor = regime match {
    case "terminalD" => new TerminalD()
    case "stage" => new Stage()
    case "output" => new Outputs()
    case "aggregates" => new Aggregates()
    case _ => new Helper()
  }

  executor.runProcessing()
  sparkSession.stop()


}