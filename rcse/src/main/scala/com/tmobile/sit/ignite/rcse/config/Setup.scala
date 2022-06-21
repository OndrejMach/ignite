package com.tmobile.sit.ignite.rcse.config

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.config.ServiceConfig

/**
 *
 * This class parses config file and stores all the parameters in case classes. It also does type conversions - especially strings to Dates
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

  val settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      app = AppConfig(
        processingDate = getDate(serviceConf.getString("config.processingDate").get),
        inputFilesPath = serviceConf.getString("config.inputFilesPath").get,
        maxDate = getDate(serviceConf.getString("config.maxDate").get),
        master = serviceConf.getString("config.master").get,
        dynamicAllocation = serviceConf.getString("config.dynamicAllocation").get
      ),
      stage = StageFilesConfig(
        clientPath = serviceConf.getString("config.stageFiles.clientPath").get,
        terminalSWPath = serviceConf.getString("config.stageFiles.terminalSWPath").get,
        imsisEncodedPath = serviceConf.getString("config.stageFiles.imsisEncodedPath").get,
        msisdnsEncodedPath = serviceConf.getString("config.stageFiles.msisdnsEncodedPath").get,
        terminalPath = serviceConf.getString("config.stageFiles.terminalPath").get,
        tacPath = serviceConf.getString("config.stageFiles.tacPath").get,
        regDerEvents = serviceConf.getString("config.stageFiles.regDerEvents").get,
        activeUsers = serviceConf.getString("config.stageFiles.activeUsers").get,
        confFile = serviceConf.getString("config.stageFiles.confFile").get,
        initUser = serviceConf.getString("config.stageFiles.initUser").get,
        initConf = serviceConf.getString("config.stageFiles.initConf").get,
        dmEventsFile = serviceConf.getString("config.stageFiles.dmEventsFile").get,
        uauFile = serviceConf.getString("config.stageFiles.uauFile").get
      ),
      output = OutputConfig(
        client = serviceConf.getString("config.outputs.client").get,
        terminal = serviceConf.getString("config.outputs.terminal").get,
        terminalSW = serviceConf.getString("config.outputs.terminalSW").get,
        activeUsers = serviceConf.getString("config.outputs.activeUsers").get,
        uauFile = serviceConf.getString("config.outputs.uauFile").get,
        initConf = serviceConf.getString("config.outputs.initConf").get,
        initUser = serviceConf.getString("config.outputs.initUser").get
      )


    )
  }
}