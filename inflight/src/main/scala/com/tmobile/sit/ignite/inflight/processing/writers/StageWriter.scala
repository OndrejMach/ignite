package com.tmobile.sit.ignite.inflight.processing.writers

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

class StageWriter(sessionData: DataFrame, completeData: DataFrame, sessionPath: String, completePath: String, date: Timestamp) extends OutputWriter with Logger{
  override def writeOutput(): Unit = {
    logger.info("Writing stage data for session report")
    sessionData
      .withColumn("year", lit(date.toLocalDateTime.getYear))
      .withColumn("month", lit(date.toLocalDateTime.getMonthValue))
      .withColumn("day", lit(date.toLocalDateTime.getDayOfMonth))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .parquet(sessionPath)

    logger.info("Writing stage data for complete report")
    completeData
      .withColumn("year", lit(date.toLocalDateTime.getYear))
      .withColumn("month", lit(date.toLocalDateTime.getMonthValue))
      .withColumn("day", lit(date.toLocalDateTime.getDayOfMonth))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .parquet(completePath)
  }
}
