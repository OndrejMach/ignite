package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

class PersistentDataProvider(settings: Settings, runConfig: RunConfig)(implicit val sparkSession: SparkSession) extends Logger {

  //TODO: broadcast - broadcast later?
  def getOldUserAgents(): DataFrame =
    broadcast(new CSVReader(settings.lookupPath.get + "User_agents.csv", header = true, delimiter = "\t").read())

  def getActivityArchives(): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("delimiter", "\\t")
      .schema(FileSchemas.activitySchema)
      .csv(settings.archivePath.get + s"activity*${runConfig.archiveFileMask}*${runConfig.natco}.csv*")
  }

  def getProvisionArchives(): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(FileSchemas.provisionSchema)
      .csv(settings.archivePath.get + s"provision*${runConfig.archiveFileMask}*${runConfig.natco}.csv*")
  }

  def getRegisterRequestArchives(): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("delimiter", "\\t")
      .schema(FileSchemas.registerRequestsSchema)
      .csv(settings.archivePath.get + s"register_requests*${runConfig.archiveFileMask}*${runConfig.natco}*.csv*")
  }
}
