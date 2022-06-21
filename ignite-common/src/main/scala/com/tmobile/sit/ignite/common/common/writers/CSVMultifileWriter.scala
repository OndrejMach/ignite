package com.tmobile.sit.ignite.common.common.writers

import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * CSV Writer class enhances with multifile writing capability. It can write several csv files having he same schema and data formas.
 * Data is defined in a map containing path as a key and data as a value.
 * @author Ondrej Machacek
 *
 * @param dataToWrite - path and dataframe to write. The result is a regular file, not folder!
 * @param mergeToSingleFile - if true a single csv file is created - no folder (as spark does it) is there - default is true
 * @param delimiter - delimiter used in the file
 * @param writeHeader - if true header is written as the first line
 * @param quote - quoting character
 * @param escape - used escape character
 * @param encoding - file text encoding
 * @param quoteMode - what is encoded basically. Read spark csv writer documentation for details.
 * @param sparkSession - implicit SparkSession for writing.
 */
class CSVMultifileWriter(dataToWrite: Map[String,DataFrame], mergeToSingleFile: Boolean = true,
                         delimiter: String = ",", writeHeader: Boolean = true,
                         quote: String = "\"", escape: String = "\\",
                         encoding: String = "UTF-8", quoteMode: String = "MINIMAL",
                         timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                         dateFormat: String = "yyyy-MM-dd",nullValue: String = "", quoteAll: String = "false")(implicit sparkSession: SparkSession) extends Writer {

  override def writeData(): Unit = {
    def write(data:DataFrame, path: String) = {
      logger.info(s"Writing data to ${path}")
      CSVWriter(data = data, path = path, delimiter=delimiter,
        writeHeader = writeHeader, quote= quote,
        escape = escape, encoding = encoding,
        quoteMode = quoteMode, timestampFormat = timestampFormat,
        dateFormat = dateFormat, mergeToSingleFile=mergeToSingleFile, nullValue = nullValue, quoteAll = quoteAll).writeData()
    }

    dataToWrite.foreach(i => write(i._2, i._1))
  }
}

object CSVMultifileWriter {
  def apply(dataToWrite: Map[String, DataFrame], mergeToSingleFile: Boolean = true,
            delimiter: String = ",", writeHeader: Boolean = true,
            quote: String = "\"", escape: String = "\\",
            encoding: String = "UTF-8", quoteMode: String = "MINIMAL",
            timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ", dateFormat: String = "yyyy-MM-dd", nullValue: String = "", quoteAll: String= "false")
    (implicit sparkSession: SparkSession): CSVMultifileWriter
  = new CSVMultifileWriter(dataToWrite,mergeToSingleFile ,delimiter, writeHeader, quote, escape, encoding, quoteMode, timestampFormat, dateFormat, nullValue, quoteAll=quoteAll)(sparkSession)
}