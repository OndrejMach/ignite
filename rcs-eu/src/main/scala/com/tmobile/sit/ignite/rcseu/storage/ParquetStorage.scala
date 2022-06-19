package com.tmobile.sit.ignite.rcseu.storage

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.Settings
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetStorage(settings: Settings)(implicit val spark: SparkSession) extends Logger {

  val parquetPath: String = settings.parquetPath.get

  def storeActivityData(date: String, data: DataFrame): Unit = {
    val targetPath = s"$parquetPath/activity"
    data.withColumn("date", lit(date)).write
      .partitionBy("date", "natco").mode("overwrite")
      .parquet(targetPath)
  }

  def storeProvisionData(date: String, data: DataFrame): Unit = {
    data.withColumn("date", lit(date)).write
      .partitionBy("date", "natco").mode("overwrite")
      .parquet(s"$parquetPath/provision")
  }

  def storeRegisterRequestsData(date: String, data: DataFrame): Unit = {
    data.withColumn("date", lit(date)).write
      .partitionBy("date", "natco").mode("overwrite")
      .parquet(s"$parquetPath/register_requests")
  }

  def storeOldUserAgents(data: DataFrame): Unit = {
    data.write.mode("overwrite").parquet(s"$parquetPath/user_agents")
  }
}
