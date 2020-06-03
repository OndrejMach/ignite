package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.ignite.hotspot.config.Settings
import org.apache.spark.sql.SparkSession

/** input datafor wina reports - Session_D basically

 */
class WinaReportsInputData(implicit sparkSession: SparkSession, settings: Settings) {

  private val processingDate = settings.appConfig.processing_date.get.toLocalDateTime

  val data =
    sparkSession
      .read
      .parquet(settings.stageConfig.session_d.get)
      .filter(s"year = '${processingDate.getYear}' or year='${processingDate.minusDays(91).getYear}'")
}
