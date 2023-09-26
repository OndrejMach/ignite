package com.tmobile.sit.ignite.common.common.writers

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try


/**
 * Parquet Writer class. An instance is able to write Parquet files according to the class parameters. It implements method writaData which writes
 * input DataFrame. Writer by default creates just one single partition and creates a Parquet file, not folder as regular Spark Parquet writer does.
 * @author Ondrej Machacek
 *
 * @param path - path and filename for the resulting file. Its a regular file, not folder!
 * @param sparkSession - implicit SparkSession for writing.
 */

class RCSEUParquetWriter(data: DataFrame,
                 path: String,
                    emptyValue: String = null)(implicit sparkSession: SparkSession) extends Logger {


  def writeParquetData(writeMode: String = "overwrite", partitionCols:Seq[String] = Seq(), numPartitions: Int = 5): Unit = {
    logger.info(s"Writing parquet data to ${path} ")

    data
      .repartition(numPartitions = numPartitions)
      .write
      .partitionBy(partitionCols: _*)
      .mode(writeMode)
      .option("partitionOverwriteMode", "dynamic")
      .option("emptyValue", emptyValue)
      .parquet(path)
  }
}



object RCSEUParquetWriter {
  def apply(data: DataFrame,
            path: String,
            emptyValue: String = null)
           (implicit sparkSession: SparkSession): RCSEUParquetWriter =

    new RCSEUParquetWriter(data,path,emptyValue)(sparkSession)
}

