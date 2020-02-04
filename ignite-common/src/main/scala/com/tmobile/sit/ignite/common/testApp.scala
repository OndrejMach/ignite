package com.tmobile.sit.ignite.common

import com.tmobile.sit.ignite.common.readers.{CSVReader, ExcelReader, MSAccessReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object testApp extends App {
  implicit val sparkSession =
    SparkSession.builder()
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
    .config("spark.app.name", "SIT ETL")
    .getOrCreate()

  //val readData= sparkSession.read.option("header", "true").csv("InputData/Statistik_H24_201903.csv")

  //readData.printSchema()

  val csvReader = new CSVReader("/Users/ondrejmachacek/data/EWHMigration//data/input/ewhr/work/rcse/TMD_HcsRcsDwh_m4sxvmvsm6hd_20190617.csv","/tmp/badRecordsPath", "|","utf-8", true)
  val dataCsv = csvReader.read()
  dataCsv.printSchema()

//ALNR	Artikel	200801	201112	201212	201312	201412	201512	201601	201602	201603	201604	201605	201606	201607	201608	201609	201610	201611	201612	201701	201702	201703	201704	201705	201706	201707	201708	201709	201710	201711	201712	201801	201802	201803	201804	201805	201806	201807	201808	201809	201810	201811	201812	201901	201902	201903	201904	201905	201906	201907	201908	201909	201910	201911	201912
  val peopleSchema = StructType(Array(
    StructField("ALNR", IntegerType, nullable = true),
    StructField("Artikel", StringType, nullable = true),
    StructField("200801", IntegerType, nullable = true),
    StructField("201112", IntegerType, nullable = true),
    StructField("201212", IntegerType, nullable = true),
    StructField("201312", IntegerType, nullable = true),
    StructField("201412", IntegerType, nullable = true),
    StructField("201512", IntegerType, nullable = true),
    StructField("201601", IntegerType, nullable = true),
    StructField("201602", IntegerType, nullable = true),
    StructField("201603", IntegerType, nullable = true),
    StructField("201604", IntegerType, nullable = true),
    StructField("201605", IntegerType, nullable = true),
    StructField("201606", IntegerType, nullable = true),
    StructField("201607", IntegerType, nullable = true),
    StructField("201608", IntegerType, nullable = true),
    StructField("201609", IntegerType, nullable = true),
    StructField("201610", IntegerType, nullable = true),
    StructField("201611", IntegerType, nullable = true),
    StructField("201612", IntegerType, nullable = true),
    StructField("201701", IntegerType, nullable = true),
    StructField("201702", IntegerType, nullable = true),
    StructField("201703", IntegerType, nullable = true),
    StructField("201704", IntegerType, nullable = true),
    StructField("201705", IntegerType, nullable = true),
    StructField("201706", IntegerType, nullable = true),
    StructField("201707", IntegerType, nullable = true),
    StructField("201708", IntegerType, nullable = true),
    StructField("201709", IntegerType, nullable = true),
    StructField("201710", IntegerType, nullable = true),
    StructField("201711", IntegerType, nullable = true),
    StructField("201712", IntegerType, nullable = true),
    StructField("201801", IntegerType, nullable = true),
    StructField("201802", IntegerType, nullable = true),
    StructField("201803", IntegerType, nullable = true),
    StructField("201804", IntegerType, nullable = true),
    StructField("201805", IntegerType, nullable = true),
    StructField("201806", IntegerType, nullable = true),
    StructField("201807", IntegerType, nullable = true),
    StructField("201808", IntegerType, nullable = true),
    StructField("201809", IntegerType, nullable = true),
    StructField("201810", IntegerType, nullable = true),
    StructField("201811", IntegerType, nullable = true),
    StructField("201812", IntegerType, nullable = true),
    StructField("201901", IntegerType, nullable = true),
    StructField("201902", IntegerType, nullable = true),
    StructField("201903", IntegerType, nullable = true),
    StructField("201904", IntegerType, nullable = true),
    StructField("201905", IntegerType, nullable = true),
    StructField("201906", IntegerType, nullable = true),
    StructField("201907", IntegerType, nullable = true),
    StructField("201908", IntegerType, nullable = true),
    StructField("201909", IntegerType, nullable = true),
    StructField("201910", IntegerType, nullable = true),
    StructField("201911", IntegerType, nullable = true),
    StructField("201912", IntegerType, nullable = true)
  ))

  val excelReader = new ExcelReader("/Users/ondrejmachacek/data/EWHMigration//data/input/ewhr/work/csmdp/ALNR-fuer-Reporting.xls", "'Preise'","!A3", Some(peopleSchema))
  val dataExcel = excelReader.read()
  dataExcel.printSchema()
  dataExcel.show(false)

  val mdbReader = new MSAccessReader("/Users/ondrejmachacek/data/EWHMigration//data/input/ewhr/work/csmdp/E200803032_201904.mdb", "AWH")
  val mdbData = mdbReader.read
  mdbData.printSchema()
}
