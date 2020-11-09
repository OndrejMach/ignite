package com.tmobile.sit.ignite

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.config.Settings
import org.apache.spark.sql.SparkSession

/**
 * Only for getting spark session
 */

package object hotspot extends Logger{

  def getSparkSession(implicit settings: Settings) = {
    logger.info("Initialising sparkSession")
    SparkSession.builder()
      .master(settings.appConfig.master.get)
      .config("spark.executor.instances", "34")
      .config("spark.executor.memory", "24g")
      .config("spark.executor.cores", "6")
      .config("spark.driver.memory", "20g")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.network.timeout", "50000s")
      .config("spark.app.name", settings.appConfig.application_name.get)
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .getOrCreate()

  }

}
