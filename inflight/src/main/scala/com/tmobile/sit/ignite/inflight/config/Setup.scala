package com.tmobile.sit.ignite.inflight.config

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.config.ServiceConfig

/**
 *
 * This class parses config file and stores all the parameters in case classes. It also does type conversions - especially strings to Timestamps
 */

class Setup(configFile: String = "inflight.conf") extends Logger {

  def getTimestamp(timestamp: Option[String]): Option[Timestamp] = {
    if (!timestamp.isDefined) {
      None
    } else {
      try {
        Some(Timestamp.valueOf(timestamp.get))
      } catch {
        case x: Exception => {
          logger.error(s"Timestamp value (${timestamp.get}) cant be parsed: ${x.getMessage}")
          None
        }
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
      appParams = ApplicationParams(
        firstDate = getTimestamp(serviceConf.getString("config.firstDate")),
        firstPlus1Date = getTimestamp(serviceConf.getString("config.firstPlus1Date")),
        minRequestDate = getTimestamp(serviceConf.getString("config.minRequestDate")),
        sparkAppName = serviceConf.getString("config.sparkAppName"),
        filteredAirlineCodes = getArray(serviceConf.getString("config.filteredAirlineCodes")),
        airlineCodesForReport = getArray(serviceConf.getString("config.airlineCodesForReport")),
        monthlyReportDate = getTimestamp(serviceConf.getString("config.monthlyReportDate"))
      ),
      input = InputFiles(
        path = serviceConf.getString("config.input.path"),
        oooidFile = serviceConf.getString("config.input.oooidFile"),
        radiusFile = serviceConf.getString("config.input.radiusFile"),
        flightlegFile = serviceConf.getString("config.input.flightlegFile"),
        aircraftFile = serviceConf.getString("config.input.aircraftFile"),
        airportFile = serviceConf.getString("config.input.airportFile"),
        realmFile = serviceConf.getString("config.input.realmFile"),
        airlineFile = serviceConf.getString("config.input.airlineFile"),
        timestampFormat = serviceConf.getString("config.input.timestampFormat")
      ),
      output = OutputFiles(
        path = serviceConf.getString("config.output.path"),
        voucherRadiusFile = serviceConf.getString("config.output.voucherRadiusFile"),
        radiusFile = serviceConf.getString("config.output.radiusFile"),
        flightLegFile = serviceConf.getString("config.output.flightLegFile"),
        airportFile = serviceConf.getString("config.output.airportFile"),
        airlineFile = serviceConf.getString("config.output.airlineFile"),
        oooiFile = serviceConf.getString("config.output.oooiFile"),
        aircraftFile = serviceConf.getString("config.output.aircraftFile"),
        vchrRadiusDailyFile = serviceConf.getString("config.output.vchrRadiusDailyFile"),
        radiusCreditDailyFile = serviceConf.getString("config.output.radiusCreditDailyFile"),
        timestampFormat = serviceConf.getString("config.output.timestampFormat"),
        excelReportsPath = serviceConf.getString("config.output.excelReportsPath")
      ),
      referenceData = StageFiles(
        path = serviceConf.getString("config.stageFiles.path"),
        voucherfile = serviceConf.getString("config.stageFiles.voucherfile"),
        orderDBFile = serviceConf.getString("config.stageFiles.orderDBFile"),
        exchangeRatesFile = serviceConf.getString("config.stageFiles.exchangeRatesFile"),
        sessionFile = serviceConf.getString("config.stageFiles.sessionFile"),
        completeFile = serviceConf.getString("config.stageFiles.completeFile")
      )

    )
  }
}