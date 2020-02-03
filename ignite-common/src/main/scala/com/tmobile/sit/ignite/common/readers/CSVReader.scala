package com.tmobile.sit.ignite.common.readers

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

//case class myCSV(id: Int, name: String)

class CSVReader(path: String, delimiter: String = ",",header: Boolean , schema: Option[StructType] = None)(implicit sparkSession: SparkSession) extends Reader {
  private def getCsvData(path: String) : DataFrame = {
    logger.info(s"Reading CSV from path ${path}")
    //TODO corrupted records - put them aside, warning on structure

    sparkSession
      .read
      .option("header", if (header) "true" else "false")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .csv(path)
  }

  override def read(): DataFrame = getCsvData(path)

}
