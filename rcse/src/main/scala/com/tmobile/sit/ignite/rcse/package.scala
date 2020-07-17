package com.tmobile.sit.ignite

import org.apache.spark.sql.SparkSession

package object rcse {
  def getSparkSession() = SparkSession.builder()
    //.appName("Test FWLog Reader")
    .master("local[*]")
    .config("spark.executor.instances", "6")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "2")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.app.name", "RCSE_processing")
    .config("spark.sql.sources.partitionOverwriteMode","dynamic")
    .getOrCreate()

}
