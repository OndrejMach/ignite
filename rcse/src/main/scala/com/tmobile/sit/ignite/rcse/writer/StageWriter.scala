package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class creates all stage files used for data outputs generation. It is a generic class storing just a single table
 * @param processingDate - date for which data is processed
 * @param stageData - data for a certain table
 * @param path - where to store it
 * @param sparkSession
 */

class StageWriter(processingDate: Date, stageData: DataFrame, path: String)(implicit sparkSession: SparkSession) extends RCSEWriter(processingDate = processingDate) {
  def writeData(): Unit = {
    logger.info(s"Writing  data to ${path}")
    writeParquet(stageData, path)
  }
}
