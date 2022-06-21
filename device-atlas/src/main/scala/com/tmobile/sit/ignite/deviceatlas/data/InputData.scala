package com.tmobile.sit.ignite.deviceatlas.data

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.SparkSession
import com.tmobile.sit.ignite.common.common.readers.CSVReader

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
}
