package com.tmobile.sit.ignite.common.common.writers

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try


/**
 * Parquet Writer class. An instance is able to write Parquet files according to the class parameters. It implements method writaData which writes
 * input DataFrame. Writer by default creates just one single partition and creates a Parquet file, not folder as regular Spark Parquet writer does.
 * @author Ondrej Machacek
 *
 * @param path - path and filename for the resulting file. Its a regular file, not folder!
 * @param mergeToSingleFile - if true a single Parquet file is created - no folder (as spark does it) is there - default is true
 * @param delimiter - delimiter used in the file
 * @param writeHeader - if true header is written as the first line
 * @param quote - quoting character
 * @param escape - used escape character
 * @param encoding - file text encoding
 * @param quoteMode - what is encoded basically. Read spark Parquet writer documentation for details.
 * @param sparkSession - implicit SparkSession for writing.
 */

class ParquetWriter(data: DataFrame,
                 path: String, mergeToSingleFile: Boolean = true,
                delimiter: String = ",", writeHeader: Boolean = true,
                quote: String = "\"", escape: String = "\\",
                encoding: String = "UTF-8", quoteMode: String = "MINIMAL",
                timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                dateFormat: String = "yyyy-MM-dd", nullValue: String = "", quoteAll: String = "false", emptyValue: String = null)(implicit sparkSession: SparkSession) extends Merger with Writer {


  def writeData() : Unit = {
    logger.info(s"Writing data to ${path} " )
    data
//    .repartition(20)
      .coalesce(1)
      .write
//      .mode(SaveMode.Append)
//      .option("maxRecordsPerFile", 10000)
//      .option("header", if (writeHeader) "true" else "false")
//      .option("sep", delimiter)
//      .option("quote", quote )
//      .option("escape", escape)
//      .option("quoteMode",quoteMode )
      .option("encoding", encoding)
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat)
      .option("nullValue", nullValue)
      .option("quoteAll",quoteAll )
      .option("emptyValue", emptyValue)
//      .parquet(path)
      .parquet(path+"_tmp")
    if (mergeToSingleFile) merge(path+"_tmp", path)
  }
}

object ParquetWriter {
  def apply(data: DataFrame,
             path: String, mergeToSingleFile: Boolean = true,
            delimiter: String = ",",
            writeHeader: Boolean = true,
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            quoteMode: String = "MINIMAL",
            timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
            dateFormat: String = "yyyy-MM-dd", nullValue: String = "", quoteAll: String = "false", emptyValue: String = null)
           (implicit sparkSession: SparkSession): ParquetWriter =

    new ParquetWriter(data,path,mergeToSingleFile ,delimiter, writeHeader, quote, escape, encoding, quoteMode, timestampFormat, dateFormat, nullValue, quoteAll, emptyValue)(sparkSession)
}

