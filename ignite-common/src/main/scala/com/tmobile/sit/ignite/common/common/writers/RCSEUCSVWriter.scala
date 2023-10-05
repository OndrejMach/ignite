package com.tmobile.sit.ignite.common.common.writers
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * CSV Writer class. An instance is able to write CSV files according to the class parameters. It implements method writaData which writes
 * input DataFrame. Writer by default creates just one single partition and creates a CSV file, not folder as regular Spark CSV writer does.
 * @author Ondrej Machacek
 *
 * @param path - path and filename for the resulting file. Its a regular file, not folder!
 * @param sparkSession - implicit SparkSession for writing.
 */

class RCSEUCSVWriter(data: DataFrame,
                         path: String,
                         delimiter: String = ",", writeHeader: Boolean = true,
                         quote: String = "\"", escape: String = "\\",
                         encoding: String = "UTF-8", quoteMode: String = "MINIMAL",
                         timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                         dateFormat: String = "yyyy-MM-dd", nullValue: String = "", quoteAll: String = "false", emptyValue: String = null)(implicit sparkSession: SparkSession) extends Merger with Writer {


  def writeData(): Unit = {
    logger.info(s"Writing CSV data to ${path} ")

    data
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "dynamic")
      .option("header", if (writeHeader) "true" else "false")
      .option("sep", delimiter)
      .option("quote", quote)
      .option("escape", escape)
      .option("quoteMode", quoteMode)
      .option("encoding", encoding)
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat)
      .option("nullValue", nullValue)
      .option("quoteAll", quoteAll)
      .option("emptyValue", emptyValue)
      .csv(path + "_tmp")

    merge(path + "_tmp", path)
  }
}



object RCSEUCSVWriter {
  def apply(data: DataFrame,
            path: String,
            delimiter: String = ",",
            writeHeader: Boolean = true,
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            quoteMode: String = "MINIMAL",
            timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
            dateFormat: String = "yyyy-MM-dd", nullValue: String = "", quoteAll: String = "false", emptyValue: String = null)
           (implicit sparkSession: SparkSession): RCSEUCSVWriter =

    new RCSEUCSVWriter(data,path,delimiter, writeHeader, quote, escape, encoding, quoteMode, timestampFormat, dateFormat, nullValue, quoteAll, emptyValue)(sparkSession)
}

