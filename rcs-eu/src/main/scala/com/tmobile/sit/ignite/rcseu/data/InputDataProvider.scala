package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import org.apache.spark.sql.{DataFrame, SparkSession}

class InputDataProvider(settings: Settings, runConfig: RunConfig)(implicit val sparkSession: SparkSession) extends Logger {

  val sourceFilePath = settings.archivePath.get

  def getActivityDf(): DataFrame = {
    if(runConfig.runMode.equals("update")) {
      logger.info("runMode: update")
      logger.info(s"Reading activity data for ${runConfig.date} and ${runConfig.tomorrowDate}")
      sparkSession.read
        .option("header", "true")
        .option("delimiter","\\t")
        .schema(FileSchemas.activitySchema)
        .csv(sourceFilePath + s"activity_${runConfig.date}*${runConfig.natco}.csv*",
          sourceFilePath + s"activity_${runConfig.tomorrowDate}*${runConfig.natco}.csv*")}
    else {
      logger.info(s"runMode: ${runConfig.runMode}, reading daily activity")
      new CSVReader(sourceFilePath + s"activity_${runConfig.date}*${runConfig.natco}.csv*",
        schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read()
    }
  }

  def getProvisionFiles(): DataFrame = {
    new CSVReader(sourceFilePath + s"provision_${runConfig.date}*${runConfig.natco}.csv*",
      schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t").read()
  }

  def getRegisterRequests(): DataFrame = {
    new CSVReader(sourceFilePath + s"register_requests_${runConfig.date}*${runConfig.natco}.csv*",
      schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t").read()
  }
}
