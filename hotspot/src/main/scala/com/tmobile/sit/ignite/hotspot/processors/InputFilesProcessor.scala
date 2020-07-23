package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.{FUTURE, FailedLoginsInputData, OrderDBInputData}
import com.tmobile.sit.ignite.hotspot.processors.fileprocessors.FailedLoginsStageProcessor
import com.tmobile.sit.ignite.hotspot.processors.staging.{CDRProcessor, OrderDBProcessor}
import com.tmobile.sit.ignite.hotspot.readers.TextReader
import com.tmobile.sit.ignite.hotspot.writers.{CDRStageWriter, FailedLoginsStageWriter, OrderDBStageFilenames, OrderDBStageWriter}
import org.apache.spark.sql.SparkSession

/**
 * Orchestration for processing of the input files
 * @param sparkSession
 * @param settings
 */

class InputFilesProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
    def process(): Unit = {
      logger.info(s"Starting processing for input date ${settings.appConfig.input_date} in the INPUT mode")
      logger.info("Initialising orderDB processor")
      val orderDBProcessor = new OrderDBProcessor(orderDBInputData = OrderDBInputData(settings.stageConfig, settings.inputConfig), maxDate = FUTURE, settings.appConfig.DES_encoder_path.get)
      logger.info("Processing orderDB data")
      val orderdDBData = orderDBProcessor.processData()
      //CDR
      logger.info("Preparing CDR input file parser")
      val reader = new TextReader(settings.inputConfig.CDR_filename.get)

      val date = settings.appConfig.input_date.get.toLocalDateTime
      logger.info("Inirialising processor for CDRs")
      val processor = new CDRProcessor(reader, Date.valueOf(LocalDate.of(date.getYear, date.getMonth, date.getDayOfMonth)), settings.appConfig.DES_encoder_path.get)
      logger.info("Processing CDR data")
      val cdrData = processor.processData()
      logger.info("CDR data processed, ready for writing")
      logger.info("Initialising Failed Logins input structures")
      val flInput = new FailedLoginsInputData()
      logger.info("Processing stage for failedLogins data")
      val flDataStage = new FailedLoginsStageProcessor(flInput.failedLogins, flInput.loginErrorCodes).runProcessing
      logger.info("writing CDR data to stage")
      new CDRStageWriter(path = settings.stageConfig.wlan_cdr_file.get, data = cdrData).writeData()
      logger.info("writing OrderDB data to stage")
      new OrderDBStageWriter(
        data = orderdDBData,
        filenames = OrderDBStageFilenames(
          wlanHotspot = settings.stageConfig.wlan_hotspot_filename.get,
          errorCodes = settings.stageConfig.error_codes_filename.get,
          mapVoucher = settings.stageConfig.map_voucher_filename.get,
          orderDb = settings.stageConfig.orderDB_filename.get
        )
      ).writeData()
      logger.info("Writing failed logins to stage")
      new FailedLoginsStageWriter(flDataStage).writeData()
    }
}
