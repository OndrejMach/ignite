package com.tmobile.sit.ignite.inflight.config

import java.sql.Timestamp

import com.tmobile.sit.common.config.GenericSettings

/**
 * Helping with verification of all the parameters. Gathers case class fields and verifies them
 */
abstract class FilesConfig extends GenericSettings {
  def isAllDefined: Boolean = {
    val fields = this.getClass.getDeclaredFields
    fields.foreach(_.setAccessible(true))
    fields.map(_.get(this).asInstanceOf[Option[String]]).map(f => f.isDefined && !f.isEmpty).reduce(_ && _)
  }
}

/**
 * Case classes for parameter groups -
 * Input files exclusively for inflight
 * Stage files - means reference data from hotspot (orderDB, voucher) and exchange rates
 * output files - where outputs are stored
 * application parameters - params needed by all the components - certain dates, filtering sets etc.
 */
case class InputFiles(path: Option[String], airportFile: Option[String],
                      oooidFile: Option[String], radiusFile: Option[String],
                      flightlegFile: Option[String], airlineFile: Option[String],
                      aircraftFile: Option[String], realmFile: Option[String],
                      timestampFormat: Option[String]) extends FilesConfig

case class StageFiles(path: Option[String], voucherfile: Option[String],
                      orderDBFile: Option[String], exchangeRatesFile: Option[String],
                      sessionFile: Option[String], completeFile: Option[String]) extends FilesConfig

case class OutputFiles(path: Option[String], radiusFile: Option[String],
                       voucherRadiusFile: Option[String], flightLegFile: Option[String],
                       airportFile: Option[String], airlineFile: Option[String],
                       oooiFile: Option[String], aircraftFile: Option[String],
                       vchrRadiusDailyFile: Option[String], radiusCreditDailyFile: Option[String],
                       excelReportsPath: Option[String], timestampFormat: Option[String]) extends FilesConfig

case class ApplicationParams(
                              firstDate: Option[Timestamp],
                              firstPlus1Date: Option[Timestamp],
                              minRequestDate: Option[Timestamp],
                              sparkAppName: Option[String],
                              filteredAirlineCodes: Option[Seq[String]],
                              airlineCodesForReport: Option[Seq[String]],
                              monthlyReportDate: Option[Timestamp]
                            ) extends GenericSettings {
  def isAllDefined = {
    firstDate.isDefined && firstPlus1Date.isDefined && minRequestDate.isDefined &&
      sparkAppName.isDefined && !sparkAppName.isEmpty && filteredAirlineCodes.isDefined &&
      airlineCodesForReport.isDefined && monthlyReportDate.isDefined
  }
}

case class Settings(
                     appParams: ApplicationParams,
                     input: InputFiles,
                     referenceData: StageFiles,
                     output: OutputFiles
                   ) extends GenericSettings {

  override def isAllDefined: Boolean = {
    appParams.isAllDefined && input.isAllDefined && output.isAllDefined && referenceData.isAllDefined
  }

  override def printAllFields(): Unit = {
    logger.info(s"${Console.RED}INPUT PARAMETERS:${Console.RESET}")
    input.printAllFields()
    logger.info(s"${Console.RED}OUTPUT PARAMETERS:${Console.RESET}")
    output.printAllFields()
    logger.info(s"${Console.RED}REFERENCE DATA PARAMETERS:${Console.RESET}")
    referenceData.printAllFields()
    logger.info(s"${Console.RED}APPLICATION PARAMETERS:${Console.RESET}")
    appParams.printAllFields()
  }
}
