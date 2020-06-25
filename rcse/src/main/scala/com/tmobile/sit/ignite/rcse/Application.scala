package com.tmobile.sit.ignite.rcse

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.{ActiveUsersToStage, ConfToStage, EventsToStage, TerminalDProcessor}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.DataFrame

object Application extends App {
  implicit val sparkSession = getSparkSession()


  val settings = Settings(terminalPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_terminal.csv",
    tacPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_tac.csv",
    maxDate = Date.valueOf(LocalDate.of(4712, 12, 31)),
    outputPath = "/Users/ondrejmachacek/tmp/rcse/cptm_ta_d_rcse_terminal.csv",
    encoderPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/shared/lib/a.out")

  //new TerminalDProcessor(settings).processData()
  //new EventsToStage(settings, Timestamp.valueOf(LocalDateTime.now())).processData()
  //new ActiveUsersToStage(Date.valueOf(LocalDate.now())).processData()
  new ConfToStage(settings, max_Date = Date.valueOf(LocalDate.of(2072,12,31)), Date.valueOf(LocalDate.now())).processData()


}