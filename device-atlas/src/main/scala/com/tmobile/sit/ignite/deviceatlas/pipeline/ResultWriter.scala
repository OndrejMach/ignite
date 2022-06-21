package com.tmobile.sit.ignite.deviceatlas.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.writers.CSVWriter
import com.tmobile.sit.ignite.deviceatlas.config.Settings
import com.tmobile.sit.ignite.deviceatlas.data.OutputData
import com.tmobile.sit.ignite.deviceatlas.writers.StageWriter
import org.apache.spark.sql.SparkSession

trait Writer extends Logger{
  def write(output: OutputData, ODATE: String): Unit
}
/**
 * The ResultWrite class is an implementation of the CSVWriter over a set of output files
 * required by the RBM pipeline. It takes into consideration the file metadata for the current
 * file date and natco, as well as the ResultsPath class because it's writing both output and
 * lookup files for the next iteration
 */
class ResultWriter(settings: Settings) (implicit sparkSession: SparkSession) extends Writer {
  override def write(outputData: OutputData, ODATE: String) =
  {
      //val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

      logger.info(s"Writing ${settings.outputPath.get}terminaldb_$ODATE.csv")
      CSVWriter(data = outputData.terminalDB,
        path = s"${settings.outputPath.get}terminaldb_$ODATE.csv",  delimiter = "|", writeHeader = false, escape = "", quote = "", encoding = "CP1250").writeData()

      logger.info(s"Writing ${settings.outputPath.get}cptm_ta_d_terminal_spec.csv")
      CSVWriter(data = outputData.d_terminal,
        path = s"${settings.outputPath.get}cptm_ta_d_terminal_spec.csv",  delimiter = "|", writeHeader = false, escape = "", quote = "").writeData()

      logger.info(s"Writing ${settings.outputPath.get}cptm_ta_d_tac.csv")
      CSVWriter(data = outputData.d_tac,
        path = s"${settings.outputPath.get}cptm_ta_d_tac.csv",  delimiter = "|", writeHeader = false, escape = "", quote = "").writeData()

      logger.info(s"Writing ${settings.outputPath.get}cptm_vi_d_tac_terminal_${ODATE}.csv")
      CSVWriter(data = outputData.cptm_vi_d_tac_terminal,
        path = s"${settings.outputPath.get}cptm_vi_d_tac_terminal_${ODATE}.csv",  delimiter = ";", writeHeader = false).writeData()

      logger.info(s"Writing ${settings.outputPath.get}cptm_ta_d_terminal_spec_${ODATE}.csv")
      CSVWriter(data = outputData.cptm_ta_d_terminal_spec,
        path = s"${settings.outputPath.get}cptm_ta_d_terminal_spec_${ODATE}.csv",  delimiter = ";", writeHeader = false, escape = "", quote = "", encoding = "CP1250").writeData()

      logger.info(s"Writing parquet files to stage")
      StageWriter(stageData = outputData.d_tac, partitioned = true,
        path = s"${settings.stagePath.get}cptm_ta_d_tac.parquet").writeData()

    }
}