package com.tmobile.sit.ignite.hotspot.writers

import com.tmobile.sit.ignite.common.common.writers.Writer
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Writer for CDR stage data
 * @param data - what to write (CDR data)
 * @param path - where to write it as parquet, partitioned by year, month, day
 */

class CDRStageWriter(data: DataFrame, path: String) extends Writer {
  override def writeData(): Unit = {
    logger.info(s"Writing stage data for CDRs to ${path}")
    data
      .repartition(1)
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .parquet(path)
  }
}
