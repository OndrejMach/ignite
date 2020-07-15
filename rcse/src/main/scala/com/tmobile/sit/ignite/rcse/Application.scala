package com.tmobile.sit.ignite.rcse

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.{AggregateUAU, InitUserAggregates}

object Application extends App {
  implicit val sparkSession = getSparkSession()

sparkSession.sparkContext.getConf.setAppName("test")


  val settings = Settings(
    inputFilesPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/input/TMD_*",
    clientPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_client.csv",
    terminalSWPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_terminal_sw.csv",
    imsisEncodedPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/input/imsis_encoded.csv",
    msisdnsEncodedPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/input/msisdns_encoded.csv",
    terminalPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_terminal.csv",
    tacPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_tac.csv",
    maxDate = Date.valueOf(LocalDate.of(4712, 12, 31)),
    outputPath = "/Users/ondrejmachacek/tmp/rcse/cptm_ta_d_rcse_terminal.csv",
    encoderPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/shared/lib/a.out",
    regDerEventsToday = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_events.TMD.20200607.reg_der.csv",
    regDerEventsYesterday = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_events.TMD.20200606.reg_der.csv.gz",
    activeUsersToday = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_active_user.TMD.20200607.csv.gz",
    activeUsersYesterday = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_active_user.TMD.20200606.csv.gz",
    confFile = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
    initUser = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_x_rcse_init_user.TMD.csv",
    initConf = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_x_rcse_init_conf.TMD.csv"
  )

  //new TerminalDProcessor(settings).processData()
  //new EventsToStage(settings, Timestamp.valueOf(LocalDateTime.now())).processData()
  //new ActiveUsersToStage(Date.valueOf(LocalDate.of(2020,6,7))).processData()
  //new ConfToStage(settings, max_Date = Date.valueOf(LocalDate.of(4712,12,31)), Date.valueOf(LocalDate.of(2020,6,8))).processData()
  //new InitConfAggregatesProcessor(Date.valueOf(LocalDate.of(2020,6,7)), settings).processData()
  //new InitUserAggregatesProcessor(Date.valueOf(LocalDate.of(2020,6,7)), settings).processData()
  new AggregateUAU(Date.valueOf(LocalDate.of(2020,6,7)), settings).processData()


}