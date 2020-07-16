package com.tmobile.sit.ignite.rcse.config

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.config.ServiceConfig

/**
 *
 * This class parses config file and stores all the parameters in case classes. It also does type conversions - especially strings to Timestamps
 */

class Setup(configFile: String = "rcse.conf") extends Logger {

  def getDate(date: String): Date = {
    try {
      val dateParsed = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyyMMdd"))
      Date.valueOf(dateParsed)
    } catch {
      case x: Exception => {
        logger.error(s"Timestamp value (${date}) cant be parsed: ${x.getMessage}")
        Date.valueOf(LocalDate.now())
      }
    }
  }

  def getArray(stringArr: Option[String]): Option[Seq[String]] = {
    if (!stringArr.isDefined) {
      None
    } else {
      Some(stringArr.get.split(","))
    }
  }


  val settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      app = AppConfig(
        processingDate = getDate(serviceConf.getString("config.processingDate").get),
        inputFilesPath = serviceConf.getString("config.inputFilesPath").get,
        maxDate = getDate(serviceConf.getString("config.maxDate").get)
      ),
      stage = StageFilesConfig(
        clientPath = serviceConf.getString("config.stageFles.clientPath").get,
        terminalSWPath = serviceConf.getString("config.stageFles.terminalSWPath").get,
        imsisEncodedPath = serviceConf.getString("config.stageFles.imsisEncodedPath").get,
        msisdnsEncodedPath = serviceConf.getString("config.stageFles.msisdnsEncodedPath").get,
        terminalPath = serviceConf.getString("config.stageFles.terminalPath").get,
        tacPath = serviceConf.getString("config.stageFles.tacPath").get,
        regDerEventsToday = serviceConf.getString("config.stageFles.regDerEventsToday").get,
        regDerEventsYesterday = serviceConf.getString("config.stageFles.regDerEventsYesterday").get,
        activeUsersToday = serviceConf.getString("config.stageFles.activeUsersToday").get,
        activeUsersYesterday = serviceConf.getString("config.stageFles.activeUsersYesterday").get,
        confFile = serviceConf.getString("config.stageFles.confFile").get,
        initUser = serviceConf.getString("config.stageFles.initUser").get,
        initConf = serviceConf.getString("config.stageFles.initConf").get,
        dmEventsFile = serviceConf.getString("config.stageFles.dmEventsFile").get,
        uauFile = serviceConf.getString("config.stageFles.uauFile").get
      ),
      output = OutputConfig(
        client=serviceConf.getString("config.outputs.client").get,
        terminal=serviceConf.getString("config.outputs.terminal").get,
        terminalSW=serviceConf.getString("config.outputs.terminalSW").get,
        activeUsers=serviceConf.getString("config.outputs.activeUsers").get,
        uauFile=serviceConf.getString("config.outputs.uauFile").get,
        initConf=serviceConf.getString("config.outputs.initConf").get,
        initUser=serviceConf.getString("config.outputs.initUser").get
      )



    )
  }
}