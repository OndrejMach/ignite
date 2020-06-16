package com.tmobile.sit.ignite.rcse

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.TerminalDProcessor

object Application extends App {
  implicit val sparkSession = getSparkSession()


  val settings = Settings(terminalPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_terminal.csv",
    tacPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_tac.csv",
    maxDate = Date.valueOf(LocalDate.of(4712, 12, 31)),
    outputPath = "/Users/ondrejmachacek/tmp/rcse/cptm_ta_d_rcse_terminal.csv")

  new TerminalDProcessor(settings).processData()

}