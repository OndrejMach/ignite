package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

class PersistentDataProvider(settings: Settings, runConfig: RunConfig)(implicit val spark: SparkSession) extends Logger {

  private val archivePath = settings.archivePath.get
  private val parquetPath = settings.parquetPath.get

  //TODO: broadcast - broadcast later?
  def getOldUserAgents: DataFrame =
    broadcast(spark.read.parquet(parquetPath + "/user_agents"))

  def getActivityArchives: DataFrame = {
    spark.read.parquet(archivePath + "/activity").filter(runConfig.archiveFilter)
  }

  def getProvisionArchives: DataFrame = {
    spark.read.parquet(archivePath + "/provision").filter(runConfig.archiveFilter)
  }

  def getRegisterRequestArchives: DataFrame = {
    spark.read.parquet(archivePath + "/register_requests").filter(runConfig.archiveFilter)
  }
}
