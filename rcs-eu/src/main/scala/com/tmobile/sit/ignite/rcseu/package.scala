package com.tmobile.sit.ignite

import org.apache.spark.sql.SparkSession

package object rcseu {
  def getSparkSession(sparkAppName: String): SparkSession = {
    //TODO[SUGGESTION] I would recommend moving all the parameters directly to the spark-submit command. Unfortunately, the current behavior does not overwrite the setup configuration.
    SparkSession.builder()
      //.appName("rcs-eu")
//      .master("local[*]")//turn on when running locally
//      .config("spark.executor.instances", "32")
//      .config("spark.executor.memory", "32g")
//      .config("spark.executor.cores", "8")
//      .config("spark.driver.memory", "32g")
//      .config("spark.driver.maxResultSize", "16g")
      .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
//      .config("spark.shuffle.service.enabled", "false")
//      .config("spark.dynamicAllocation.enabled", "false")
//      .config("spark.yarn.executor.memoryOverhead", "5GB")
      .config("spark.shuffle.memoryFraction=", "0")
      .config("spark.reducer.maxReqsInFlight", "1")
      .config("spark.shuffle.io.retryWait", "60s")
      .config("spark.shuffle.io.maxRetries", "10")
      .config("spark.app.name", sparkAppName)
      .getOrCreate()
  }
}
