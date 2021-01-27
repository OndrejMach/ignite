package com.tmobile.sit.ignite

import com.tmobile.sit.ignite.deviceatlas.config.Settings
import org.apache.spark.sql.SparkSession

package object deviceatlas {
  def getSparkSession(implicit settings: Settings): SparkSession = {
    SparkSession.builder()
      //.appName("deviceatlas-ignite")
      //.master("local[*]")
      //.config("spark.sql.broadcastTimeout", "36000")
      .config("spark.executor.instances", "4")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "8")
      .config("spark.driver.memory", "10g")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.app.name", settings.appName.get)
      .getOrCreate()
  }
}
