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
 * @param sparkSession - implicit SparkSession for writing.
 */

class ParquetWriter(data: DataFrame,
                 path: String, mergeToSingleFile: Boolean = true,
                    emptyValue: String = null)(implicit sparkSession: SparkSession) extends Merger with Writer {


  def writeData() : Unit = {
    logger.info(s"Writing data to ${path} " )
    data
      .coalesce(1)
      .write
      .option("emptyValue", emptyValue)
      .parquet(path+"_tmp")
    if (mergeToSingleFile) merge(path+"_tmp", path)
  }

  def writeParquetData(): Unit = {
    logger.info(s"Writing parquet data to ${path} ")
    data
      .write
      .mode("overwrite")
      .option("emptyValue", emptyValue)
      .parquet(path)
  }
}



object ParquetWriter {
  def apply(data: DataFrame,
            path: String,
            mergeToSingleFile: Boolean = true,
            emptyValue: String = null)
           (implicit sparkSession: SparkSession): ParquetWriter =

    new ParquetWriter(data,path,mergeToSingleFile, emptyValue)(sparkSession)
}

