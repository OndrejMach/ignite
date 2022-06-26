package com.tmobile.sit.ignite.rcseu.storage

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.Settings
import com.tmobile.sit.ignite.rcseu.data.{FileSchemas, dataTimeZone, dateFormatPattern}
import org.apache.spark.sql.functions.{col, from_utc_timestamp, lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvInputFilesReader(settings: Settings)(implicit val spark: SparkSession) extends Logger {

  val sourceFilePath: String = settings.inputPath.get

  private def checkPatternExists(pattern: String): Boolean = {

    println(s"Checking for pattern $pattern")

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsPath = new org.apache.hadoop.fs.Path(pattern)
    fs.globStatus(hdfsPath).length > 0
  }

  def checkActivityDataExists(date: String, natco: String): Boolean =
    checkPatternExists(sourceFilePath + s"activity_${date}*${natco}.csv*")

  def getActivityData(date: String, natco: String): DataFrame = {
      new CSVReader(sourceFilePath + s"activity_${date}*${natco}.csv*",
        schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t")
        .read().withColumn("natco", lit(natco))
        .withColumn("date",
          to_date(from_utc_timestamp(col("creation_date"), dataTimeZone), dateFormatPattern))
  }

  def checkProvisionDataExists(date: String, natco: String): Boolean =
    checkPatternExists(sourceFilePath + s"provision_${date}*${natco}.csv*")

  def getProvisionData(date: String, natco: String): DataFrame = {
    new CSVReader(sourceFilePath + s"provision_${date}*${natco}.csv*",
      schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t")
      .read().withColumn("natco", lit(natco)).withColumn("date", to_date(lit(date), dateFormatPattern))
  }

  def checkRegisterRequestsExists(date: String, natco: String): Boolean =
    checkPatternExists(sourceFilePath + s"register_requests_${date}*${natco}.csv*")

  def getRegisterRequestsData(date: String, natco: String): DataFrame = {
    new CSVReader(sourceFilePath + s"register_requests_${date}*${natco}.csv*",
      schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t")
      .read().withColumn("natco", lit(natco)).withColumn("date", to_date(lit(date), dateFormatPattern))
  }

  def getOldUserAgents: DataFrame =
    new CSVReader(settings.lookupPath.get + "User_agents.csv", header = true, delimiter = "\t").read()

}
