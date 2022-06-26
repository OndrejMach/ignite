package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.RunMode
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class InputDataProvider(settings: Settings, runConfig: RunConfig)(implicit val sparkSession: SparkSession) extends Logger {

  private val parquetFilePath: String = settings.parquetPath.get
  private val dateStr = runConfig.date.format(dateFormatter)
  private val tomorrowStr = runConfig.tomorrowDate.format(dateFormatter)

  def getActivityDf: DataFrame = {
    if(runConfig.runMode.equals(RunMode.UPDATE)) {
      logger.info("runMode: update")
      logger.info(s"Reading activity data for ${dateStr} and ${tomorrowStr}")
      val dateData = sparkSession.read.option("basePath", s"$parquetFilePath/activity")
        .parquet(parquetFilePath + s"activity/natco=${runConfig.natco}/date=${dateStr}")
      val tomorrowData = sparkSession.read.option("basePath", s"$parquetFilePath/activity")
        .parquet(parquetFilePath + s"activity/natco=${runConfig.natco}/date=${tomorrowStr}")
      dateData union tomorrowData
    }
    else {
      logger.info(s"runMode: ${runConfig.runMode}, reading daily activity")
      sparkSession.read.option("basePath", s"$parquetFilePath/activity")
        .parquet(parquetFilePath + s"activity/natco=${runConfig.natco}/date=${dateStr}")
    }
  }

  def getProvisionFiles: DataFrame = {
    sparkSession.read.option("basePath", s"$parquetFilePath/provision")
      .parquet(parquetFilePath + s"provision/natco=${runConfig.natco}/date=${dateStr}")
  }

  def getRegisterRequests: DataFrame = {
    sparkSession.read.option("basePath", s"$parquetFilePath/register_requests")
      .parquet(parquetFilePath + s"register_requests/natco=${runConfig.natco}/date=${dateStr}")
  }
}
