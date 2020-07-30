package com.tmobile.sit.ignite.deviceatlas.data

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.SparkSession
import com.tmobile.sit.common.readers.CSVReader

class InputData (inputPath : String, fileName: String)(implicit sparkSession : SparkSession) extends Logger {
  val deviceAtlas = {
    val file = inputPath + fileName
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = true,
      schema = Some(FileStructures.deviceMap),
      delimiter = "|")
      .read()
  }

  val d_terminal_spec = {
    val file = inputPath + "cptm_ta_d_terminal_spec.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.cptm_term_spec),
      delimiter = "|",
      encoding = "CP1250")
      .read()
  }

  val d_tac = {
    val file = inputPath + "cptm_ta_d_tac.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.cptm_ta_d_tac),
      delimiter = "|",
      encoding = "CP1250")
      .read()
  }


}
