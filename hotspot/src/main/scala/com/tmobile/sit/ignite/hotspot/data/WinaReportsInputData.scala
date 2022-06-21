package com.tmobile.sit.ignite.hotspot.data

import java.time.format.DateTimeFormatter

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.hotspot.config.Settings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** input datafor wina reports - Session_D basically

 */
class WinaReportsInputData(implicit sparkSession: SparkSession, settings: Settings) extends Logger{
  import sparkSession.implicits._
  private val processingDate = settings.appConfig.processing_date.get.toLocalDateTime
  private val lowerBound = processingDate.minusDays(100).format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInt


  val data = {
    logger.info(s"Reading data from ${settings.stageConfig.session_d.get} for date lowe bound ${lowerBound}")

    sparkSession
      .read
      .parquet(settings.stageConfig.session_d.get)
      .filter($"date"  >= lit(lowerBound))
  }
}
