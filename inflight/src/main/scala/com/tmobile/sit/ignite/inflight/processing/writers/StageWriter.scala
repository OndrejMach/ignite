package com.tmobile.sit.ignite.inflight.processing.writers

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Application also writes parquets for storing data needed by monthly excel reports. Partitioned by year/month/day in the standard spark way.
 * @param sessionData - data for session report
 * @param completeData - data for complete report
 * @param sessionPath - where data for daily session data should be stored
 * @param completePath - path where daily complete reports should be stored
 * @param date - what aprticular data we are storing now
 */

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
