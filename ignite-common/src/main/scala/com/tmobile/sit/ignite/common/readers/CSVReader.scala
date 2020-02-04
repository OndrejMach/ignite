package com.tmobile.sit.ignite.common.readers

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

//case class myCSV(id: Int, name: String)

class CSVReader(path: String, badRecordsPath: String, delimiter: String = ",", header: Boolean, schema: Option[StructType] = None)(implicit sparkSession: SparkSession) extends Reader {
  private def getCsvData(path: String): DataFrame = {
    logger.info(s"Reading CSV from path ${path}, bad records will be stored in ${badRecordsPath}")
    val reader = sparkSession
      .read
      .option("badRecordsPath", badRecordsPath)
      .option("header", if (header) "true" else "false")
      .option("delimiter", delimiter)

    val schemaUpdated = if (schema.isDefined) {
      reader.schema(schema.get)
    } else {
      logger.warn("Schema file not defined, trying to infer one")
      reader.option("inferSchema", "true")
    }

    schemaUpdated.csv(path)

  }

  override def read(): DataFrame = getCsvData(path)

}
