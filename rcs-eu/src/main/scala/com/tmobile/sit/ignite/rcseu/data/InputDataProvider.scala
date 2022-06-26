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
      val dateData = sparkSession.read
        .parquet(parquetFilePath + s"activity/natco=${runConfig.natco}/date=${dateStr}")
        .withColumn("date", lit(dateStr)).withColumn("natco", lit(runConfig.natco))
      val tomorrowData = sparkSession.read
        .parquet(parquetFilePath + s"activity/natco=${runConfig.natco}/date=${tomorrowStr}")
        .withColumn("date", lit(dateStr)).withColumn("natco", lit(runConfig.natco))
      dateData union tomorrowData
    }
    else {
      logger.info(s"runMode: ${runConfig.runMode}, reading daily activity")
      sparkSession.read
        .parquet(parquetFilePath + s"activity/natco=${runConfig.natco}/date=${dateStr}")
        .withColumn("date", lit(dateStr)).withColumn("natco", lit(runConfig.natco))
    }
  }

  def getProvisionFiles: DataFrame = {
    sparkSession.read
      .parquet(parquetFilePath + s"provision/natco=${runConfig.natco}/date=${dateStr}")
      .withColumn("date", lit(dateStr)).withColumn("natco", lit(runConfig.natco))
  }

  def getRegisterRequests: DataFrame = {
    sparkSession.read
      .parquet(parquetFilePath + s"register_requests/natco=${runConfig.natco}/date=${dateStr}")
      .withColumn("date", lit(dateStr)).withColumn("natco", lit(runConfig.natco))
  }
}
