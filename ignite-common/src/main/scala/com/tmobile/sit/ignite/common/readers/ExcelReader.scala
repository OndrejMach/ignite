package com.tmobile.sit.ignite.common.readers

import com.crealytics.spark.excel._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.types.StructType

class ExcelReader(path: String, sheetName: String = "sheetName", schema: StructType)(implicit sparkSession: SparkSession) extends Reader {
  //split re
  override def read(): DataFrame = {
   logger.info(s"Reading excel file from path ${path}")
    sparkSession.read.excel(
      useHeader = true, // Required
      dataAddress = sheetName, // Optional, default: "A1"'My Sheet'!B3:C35
      treatEmptyValuesAsNulls = true,  // Optional, default: true
      inferSchema = true,  // Optional, default: false
      //addColorColumns = true,  // Optional, default: false
      //timestampFormat = "MM-dd-yyyy HH:mm:ss",  // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      //maxRowsInMemory = 20, // Optional, default None. If set, uses a streaming reader which can help with big files
      excerptSize = 10 // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      //workbookPassword = "pass"  // Optional, default None. Requires unlimited strength JCE for older JVMs
    )
      .schema(schema) // Optional, default: Either inferred schema, or all columns are Strings
      .load(path)
  }
}
