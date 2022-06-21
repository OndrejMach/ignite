package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit

/**
 * abstract class implementing a method for writing parquets (for stage files) it requires processing data used for partitioning
 * @param processingDate - used for partitioning
 */

abstract class RCSEWriter(processingDate: Date) extends Logger {
  def writeParquet(data: DataFrame, path: String) = {
    logger.info(s"Writing to path ${path}")


    //data.show(false)

    val toWrite = data
      .withColumn("date", lit(processingDate))

    //toWrite.show(false)

    toWrite
      .write
      .partitionBy("date")
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  def writeData(): Unit

}
