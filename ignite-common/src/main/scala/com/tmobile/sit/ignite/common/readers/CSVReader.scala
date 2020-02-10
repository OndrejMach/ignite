package com.tmobile.sit.ignite.common.readers

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

//case class myCSV(id: Int, name: String)

class CSVReader(path: String,
                header: Boolean,
                badRecordsPath: Option[String] = None,
                delimiter: String = ",",
                quote: String = "\"",
                escape: String = "\\",
                encoding: String = "UTF-8",
                schema: Option[StructType] = None)
               (implicit sparkSession: SparkSession) extends Reader {

  private def getCsvData(path: String): DataFrame = {
    logger.info(s"Reading CSV from path ${path}")
    val reader = sparkSession
      .read
      .option("header", if (header) "true" else "false")
      .option("quote", quote )
      .option("escape", escape)
      .option("delimiter", delimiter)
      .option("encoding",encoding )

    val invalidHandling = if (badRecordsPath.isDefined) {
      logger.info(s"Bad records to be stored in ${badRecordsPath.get}")
      reader.option("badRecordsPath", badRecordsPath.get)
    }
    else reader.option("mode", "DROPMALFORMED")

    val schemaUpdated = if (schema.isDefined) {
      invalidHandling.schema(schema.get)
    } else {
      logger.warn("Schema file not defined, trying to infer one")
      invalidHandling.option("inferschema", "true")
    }

    schemaUpdated.csv(path)

  }

  override def read(): DataFrame = getCsvData(path)

}
