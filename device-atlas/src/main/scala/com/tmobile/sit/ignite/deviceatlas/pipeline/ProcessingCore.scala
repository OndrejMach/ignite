package com.tmobile.sit.ignite.deviceatlas.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.deviceatlas.config.Settings
import com.tmobile.sit.ignite.deviceatlas.data.{InputData, LookupData, OutputData}
import org.apache.spark.sql.SparkSession

/**
 * Class trait/interface which needs to be implemented
 */
trait ProcessingCore extends Logger {
  def process(inputData: InputData, lookupData: LookupData, settings: Settings, ODATE: String) : OutputData
}

/**
 * This class implements the core processing method which creates and updates dimensional data
 * by calling the respective classes and methods
 */
class CoreProcessing(implicit sparkSession: SparkSession) extends ProcessingCore {

  /**
   * The process method implements the main logic and creates the output structure
   */
  override def process(inputData: InputData, lookupData: LookupData, settings: Settings, ODATE: String): OutputData = {
    logger.info("Executing  processing core")
    val terminalDB = new TerminalDB()
    val dimFiles = new Dimensions()
    val files_to_send = new ExportOutputs()

    val updatedTerminalDB = terminalDB.update(inputData, lookupData, ODATE, settings.outputPath.get).cache()

    val updatedD_terminal = dimFiles.update_d_terminal(updatedTerminalDB, lookupData.d_terminal_spec, settings.workPath.get, ODATE).cache()
    val updatedD_tac = dimFiles.update_d_tac(updatedTerminalDB, lookupData.d_tac, settings.workPath.get, ODATE).cache()

    val cptm_ta_d_terminal_spec = files_to_send.generateSpec(updatedD_terminal, ODATE, settings.outputPath.get)
    val cptm_vi_d_tac_terminal = files_to_send.generateTac(updatedD_tac, ODATE, settings.outputPath.get)

    OutputData(updatedTerminalDB,
      updatedD_terminal,
      updatedD_tac,
      cptm_vi_d_tac_terminal,
      cptm_ta_d_terminal_spec)
  }
}

