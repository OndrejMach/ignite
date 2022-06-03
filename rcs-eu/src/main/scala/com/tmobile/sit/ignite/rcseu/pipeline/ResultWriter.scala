package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The ResultWrite class is an implementation of the CSVWriter over a set of output files
 * required by the RBM pipeline. It takes into consideration the file metadata for the current
 * file date and natco, as well as the ResultsPath class because it's writing both output and
 * lookup files for the next iteration
 */
class ResultWriter(settings: Settings, outputDir: String)(implicit sparkSession: SparkSession) extends Logger {
  def writeWithFixedEmptyDFs(data: DataFrame, fileName: String): Unit = {
    import sparkSession.implicits._
    if (data.isEmpty) {
      val line = Seq(data.columns.mkString("\t"))
      val df = line.toDF()
      CSVWriter(df, outputDir + fileName, delimiter = "\t", writeHeader = false, quote = "").writeData()
    } else {
      CSVWriter(data, outputDir + fileName, delimiter = "\t", writeHeader = true).writeData()
    }
  }
}
