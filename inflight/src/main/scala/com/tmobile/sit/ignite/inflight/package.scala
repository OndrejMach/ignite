package com.tmobile.sit.ignite

import com.tmobile.sit.ignite.inflight.config.Settings
import org.apache.spark.sql.SparkSession

/**
 * helper methods for common actions - getting sparkSession and transforming seconds to more human readable form hours:minutes:seconds
 */
package object inflight {
  def getSparkSession(settings: Settings) = SparkSession.builder()
    //.appName("Test FWLog Reader")
    .master(settings.appParams.master.get)
    .config("spark.executor.instances", "8")
    .config("spark.executor.memory", "12g")
    .config("spark.executor.cores", "4")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.network.timeout", "50000s")
    .config("spark.sql.sources.partitionOverwriteMode","dynamic")
    .config("spark.app.name", "inflight_processing")
    .getOrCreate()


  def translateSeconds = (secs: Long) => {
    def pad(n: Long): String = {
      if (n < 10) "0" + n.toString else n.toString
    }
    if (secs <= 0) {
      "00:00:00"
    } else {
      val hours = secs / 3600
      val minutes = secs % 3600 / 60
      val seconds = (secs % 3600) % 60
      s"${pad(hours)}:${pad(minutes)}:${pad(seconds)}"
    }
  }
}
