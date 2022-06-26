package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import org.apache.spark.sql.functions.{broadcast, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PersistentDataProvider(settings: Settings, runConfig: RunConfig, natco: String)(implicit val spark: SparkSession) extends Logger {

  private val archivePath = settings.archivePath.get
  private val parquetPath = settings.parquetPath.get

  def getOldUserAgents: DataFrame =
    broadcast(spark.read.parquet(parquetPath + "/user_agents"))

  def getActivityArchives: DataFrame = {
    spark.read.option("basePath", s"$archivePath/activity")
      .parquet(s"$archivePath/activity/natco=$natco/date=${runConfig.archivePath}")
  }

  def getProvisionArchives: DataFrame = {
    spark.read.option("basePath", s"$archivePath/provision")
      .parquet(s"$archivePath/provision/natco=$natco/date=${runConfig.archivePath}")
  }

  def getRegisterRequestArchives: DataFrame = {
    spark.read.option("basePath", s"$archivePath/register_requests")
      .parquet(s"$archivePath/register_requests/natco=$natco/date=${runConfig.archivePath}")
  }
}
