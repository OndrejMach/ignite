package com.tmobile.sit.ignite.common.readers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class CSVReader(path: String, header: Boolean = false)(implicit sparkSession: SparkSession) extends Reader {
  private def getCsvData(path: String) : DataFrame = {
    logger.info(s"Reading CSV from path ${path}")
    sparkSession
      .read
      .option("header", if (header) "true" else "false")
      .csv(path)
  }

  override def read(): DataFrame = getCsvData(path)
}
