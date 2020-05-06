package com.tmobile.sit.ignite.hotspot.writers

import com.tmobile.sit.common.writers.{CSVWriter, Writer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.tmobile.sit.ignite.common.implicits.TransformDataFrameColumns.TransformColumnNames
import com.tmobile.sit.ignite.hotspot.data.OutputStructures

class SessionDWriter(path: String, data: DataFrame)(implicit sparkSession: SparkSession) extends Writer {
  override def writeData(): Unit =
  {
    CSVWriter(path = path,
      data = data.select(OutputStructures.SESSION_D_OUTPUT_COLUMNS.head, OutputStructures.SESSION_D_OUTPUT_COLUMNS.tail: _*).columnsToUpperCase(),
      delimiter = "|"
    ).writeData()
  }
}
