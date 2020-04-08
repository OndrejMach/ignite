package com.tmobile.sit.ignite

import org.apache.spark.sql.SparkSession

/**
 * helper methods for common actions - getting sparkSession and transforming seconds to more human readable form hours:minutes:seconds
 */
package object inflight {
  def getSparkSession() = SparkSession.builder()
    //.appName("Test FWLog Reader")
    .master("local[*]")
    .config("spark.executor.instances", "4")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "1")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.app.name", "inflight_processing")
    .getOrCreate()


  def translateSeconds = (secs: Long) => {
    def pad(n: Long): String = {
      if (n < 10) "0" + n.toString else n.toString
    }

    val hours = secs / 3600
    val minutes = secs % 3600 / 60
    val seconds = (secs % 3600) % 60

    s"${pad(hours)}:${pad(minutes)}:${pad(seconds)}"
  }
}
