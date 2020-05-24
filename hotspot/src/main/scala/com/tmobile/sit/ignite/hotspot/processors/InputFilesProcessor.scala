package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.ignite.hotspot.config.{AppConfig, InputConfig, Settings, StageConfig}
import com.tmobile.sit.ignite.hotspot.data.{FUTURE, OrderDBInputData}
import com.tmobile.sit.ignite.hotspot.processors.staging.{CDRProcessor, OrderDBProcessor}
import com.tmobile.sit.ignite.hotspot.readers.TextReader
import com.tmobile.sit.ignite.hotspot.writers.{CDRStageWriter, OrderDBStageFilenames, OrderDBStageWriter}
import org.apache.spark.sql.SparkSession

class InputFilesProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
    def process(): Unit = {
      logger.info("Initialising orderDB processor")
      val orderDBProcessor = new OrderDBProcessor(orderDBInputData = OrderDBInputData(settings.stageConfig, settings.inputConfig), maxDate = FUTURE, settings.appConfig.DES_encoder_path.get)
      logger.info("Processing orderDB data")
      val orderdDBData = orderDBProcessor.processData()
      //CDR
      logger.info("Preparing CDR input file parser")
      val reader = new TextReader(settings.inputConfig.CDR_filename.get)

      val date = settings.appConfig.processing_date.get.toLocalDateTime
      logger.info("Inirialising processor for CDRs")
      val processor = new CDRProcessor(reader, Date.valueOf(LocalDate.of(date.getYear, date.getMonth, date.getDayOfMonth)))
      logger.info("Processing CDR data")
      val cdrData = processor.processData()
      logger.info("CDR data processed, ready for writing")
      new CDRStageWriter(path = settings.stageConfig.wlan_cdr_file.get, data = cdrData).writeData()
      new OrderDBStageWriter(data = orderdDBData, filenames = OrderDBStageFilenames()).writeData()

    }
}
