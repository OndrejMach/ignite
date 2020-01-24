package com.tmobile.sit.ignite.common

import org.apache.spark.sql.SparkSession

object testApp extends App {
  val sparkSession = SparkSession.builder()
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
    .config("spark.app.name", "AParty fraud detection ETL")
    .getOrCreate()

  val readData= sparkSession.read.option("header", "true").csv("InputData/Statistik_H24_201903.csv")

  readData.printSchema()
}
