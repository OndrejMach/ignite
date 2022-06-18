package com.tmobile.sit.ignite.rcseu.storage

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetStorage(settings: Settings)(implicit val spark: SparkSession) extends Logger {

  val parquetPath: String = settings.parquetPath.get

  def storeActivityData(date: String, data: DataFrame): Unit = {
    data.write
      .partitionBy("natco").mode("overwrite")
      .parquet(s"$parquetPath/activity/date=$date")
  }

  def storeProvisionData(date: String, data: DataFrame): Unit = {
    data.write
      .partitionBy("natco").mode("overwrite")
      .parquet(s"$parquetPath/provision/date=$date")
  }

  def storeRegisterRequestsData(date: String, data: DataFrame): Unit = {
    data.write
      .partitionBy("natco").mode("overwrite")
      .parquet(s"$parquetPath/register_requests/date=$date")
  }

  def storeOldUserAgents(data: DataFrame): Unit = {
    data.write.mode("overwrite").parquet(s"$parquetPath/user_agents")
  }
}
