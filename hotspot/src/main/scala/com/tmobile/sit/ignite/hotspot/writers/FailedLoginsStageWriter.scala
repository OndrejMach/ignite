package com.tmobile.sit.ignite.hotspot.writers

import com.tmobile.sit.common.writers.Writer
import com.tmobile.sit.ignite.hotspot.config.Settings
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class FailedLoginsStageWriter(flStageData: DataFrame)(implicit settings: Settings, sparkSession: SparkSession ) extends Writer{
  override def writeData(): Unit = {
    logger.info(s"Writing loginError stage data to ${settings.stageConfig.failed_logins_input.get}")
    flStageData
      .repartition(1)
      .write
      .partitionBy("login_date")
      .mode(SaveMode.Append)
      .parquet(settings.stageConfig.failed_logins_input.get)
  }
}
