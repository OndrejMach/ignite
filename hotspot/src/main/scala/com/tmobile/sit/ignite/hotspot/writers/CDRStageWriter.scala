package com.tmobile.sit.ignite.hotspot.writers

import com.tmobile.sit.common.writers.Writer
import org.apache.spark.sql.{DataFrame, SaveMode}

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
