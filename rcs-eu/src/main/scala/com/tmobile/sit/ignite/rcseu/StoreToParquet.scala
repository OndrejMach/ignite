package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import com.tmobile.sit.ignite.rcseu.data.dateFormatter
import com.tmobile.sit.ignite.rcseu.storage.ParquetStorage
import com.tmobile.sit.ignite.rcseu.storage.CsvInputFilesReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DataKind extends Enumeration {
  type DataKind = Value

  val DAILY, PERSISTENT, ALL = Value
}

object StoreToParquet extends App with Logger {
  require(args.length == 2, "Wrong argument. Usage: ... <date:yyyy-mm-dd> <runFor:daily|persistent|all>")

  val dateString = args(0)
  val date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val yesterday = date.minusDays(1)
  val runFor = args(1)
  val runMode = if (runFor == "daily") DataKind.DAILY
                else if (runFor == "persistent") DataKind.PERSISTENT
                else if (runFor == "all") DataKind.ALL
  else throw new IllegalArgumentException(s"$runFor parameter not recognized. Options are <runFor:daily|persistent>")

  val settings: Settings = Settings.loadFile(Settings.getConfigFile)

  logger.info(s"Date: $date")

  // Get settings and create spark session
  implicit val sparkSession: SparkSession = getSparkSession(settings.appName.get)
  val csvDataFiles = new CsvInputFilesReader(settings)
  val parquetWriter = new ParquetStorage(settings)

  if (List(DataKind.DAILY, DataKind.ALL).contains(runMode)) {
    val activities = RunConfig.natcoIdMap.keys.map(natco => {
      val dayActivity = csvDataFiles.getActivityData(date.format(dateFormatter), natco)
      val yesterdayActivity = csvDataFiles.getActivityData(yesterday.format(dateFormatter), natco)
      dayActivity union yesterdayActivity
    }).reduce(_ union _)

    val provisionData = RunConfig.natcoIdMap.keys.map(natco => {
      csvDataFiles.getProvisionData(date.format(dateFormatter), natco)
    }).reduce(_ union _)

    val registerRequestsData =  RunConfig.natcoIdMap.keys.map(natco => {
      csvDataFiles.getRegisterRequestsData(date.format(dateFormatter), natco)
    }).reduce(_ union _)

    parquetWriter.storeActivityData(date.format(dateFormatter),
      activities.select(col("creation_date") === date.format(dateFormatter)))

    parquetWriter.storeActivityData(yesterday.format(dateFormatter),
      activities.select(col("creation_date") === yesterday.format(dateFormatter)))

    parquetWriter.storeProvisionData(date.format(dateFormatter), provisionData)

    parquetWriter.storeRegisterRequestsData(date.format(dateFormatter), registerRequestsData)
  }

  if (List(DataKind.PERSISTENT, DataKind.ALL).contains(runMode)) {
    val oldUserAgents = csvDataFiles.getOldUserAgents()
    parquetWriter.storeOldUserAgents(oldUserAgents)
  }
}
