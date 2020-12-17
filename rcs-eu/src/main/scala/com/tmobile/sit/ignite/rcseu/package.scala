package com.tmobile.sit.ignite

import org.apache.spark.sql.SparkSession

package object rcseu {
  def getSparkSession(sparkAppName: String): SparkSession = {
    SparkSession.builder()
      //.appName("rcs-eu")
      .master("local[*]")//turn on when running locally
      .config("spark.executor.instances", "2")
      .config("spark.executor.memory", "8g")
      .config("spark.executor.cores", "2")
      .config("spark.driver.memory", "10g")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
      .config("spark.shuffle.service.enabled", "false")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.app.name", sparkAppName)
      .getOrCreate()
  }
}
