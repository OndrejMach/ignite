package com.tmobile.sit.ignite

import org.apache.spark.sql.SparkSession

/**
 * here spark session is created with certain parameters. The last one (timeout) tries to fix connection closing issues on the CDRs cluster
 */

package object rcse {
  def getSparkSession(master: String, dynamicAllocation: String) = SparkSession.builder()
    //.appName("Test FWLog Reader")
    .master(master)
    .config("spark.executor.instances", "30")
    .config("spark.executor.memory", "16g")
    .config("spark.executor.cores", "2")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
    .config("spark.dynamicAllocation.enabled", dynamicAllocation)
    .config("spark.app.name", "RCSE_processing")
    .config("spark.sql.sources.partitionOverwriteMode","dynamic")
    .config("spark.yarn.executor.memoryOverhead", "5g")
    .config("spark.network.timeout", "1200s")
    .config("spark.executor.heartbeatInterval","120s")
    //.config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
}
