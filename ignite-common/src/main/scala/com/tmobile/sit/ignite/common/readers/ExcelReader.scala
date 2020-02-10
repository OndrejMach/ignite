package com.tmobile.sit.ignite.common.readers

import com.crealytics.spark.excel._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.types.StructType

class ExcelReader(path: String,
                  sheetName: String = "",
                  cellRange: String = "!A1",
                  schema: Option[StructType] = None,
                  dateFormat: String = "yyyy-mm-dd hh:mm:ss")
                 (implicit sparkSession: SparkSession) extends Reader {
  override def read(): DataFrame = {
    logger.info(s"Reading excel file from path ${path}, sheet-cellRange: ${sheetName}-${cellRange}")
    val dfReader = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", s"${sheetName}${cellRange}") // Optional, default: "A1"
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("timestampFormat", dateFormat) // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]

    val withSchema = if (schema.isDefined) {
      dfReader
        .schema(schema.get)
        .option("inferSchema", "false")
    } else {
      logger.warn("Schema not defined, trying to infer one")
      dfReader.option("inferSchema", "true")
    }

    withSchema
      .load(path)
  }
}
