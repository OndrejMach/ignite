package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import com.tmobile.sit.ignite.rcseu.data.dateFormatter
import com.tmobile.sit.ignite.rcseu.storage.{CsvInputFilesReader, ParquetStorage}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DataKind extends Enumeration {
  type DataKind = Value

  val DAILY, PERSISTENT, ALL = Value
}

object StoreToParquet extends App with Logger {
  require(args.length == 2, "Wrong argument count. Usage: ... <date:yyyy-mm-dd> <runFor:daily|persistent|all>")

  val dateString = args(0)
  val date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val yesterday = date.minusDays(1)
  val runFor = args(1)
  val runMode = if (runFor == "daily") DataKind.DAILY
  else if (runFor == "persistent") DataKind.PERSISTENT
  else if (runFor == "all") DataKind.ALL
  else throw new IllegalArgumentException(s"$runFor parameter not recognized. Options are <runFor:daily|persistent>")

  val natcoList = RunConfig.natcoIdMap.keys

  val settings: Settings = Settings.loadFile(Settings.getConfigFile)

  logger.info(s"Date: $date")

  implicit val sparkSession: SparkSession = getSparkSession(settings.appName.get)
  sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  val csvDataFiles = new CsvInputFilesReader(settings)
  val parquetWriter = new ParquetStorage(settings)

  if (List(DataKind.DAILY, DataKind.ALL).contains(runMode)) {
    val activities = natcoList
      .filter(natco => csvDataFiles.checkActivityDataExists(date.format(dateFormatter), natco))
      .map(natco => {
        val dayActivity = csvDataFiles.getActivityData(date.format(dateFormatter), natco)
        val yesterdayActivity = if (csvDataFiles.checkActivityDataExists(yesterday.format(dateFormatter), natco)) {
          csvDataFiles.getActivityData(yesterday.format(dateFormatter), natco)
        } else {
          sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], dayActivity.schema)
        }
        dayActivity union yesterdayActivity
    }).filter(! _.isEmpty).reduce(_ union _)

    val provisionData = natcoList
      .filter(natco => csvDataFiles.checkProvisionDataExists(date.format(dateFormatter), natco))
      .map(natco => {
      csvDataFiles.getProvisionData(date.format(dateFormatter), natco)
    }).reduce(_ union _)

    val registerRequestsData = natcoList
      .filter(natco => csvDataFiles.checkRegisterRequestsExists(date.format(dateFormatter), natco))
      .map(natco => {
      csvDataFiles.getRegisterRequestsData(date.format(dateFormatter), natco)
    }).reduce(_ union _)

    activities.printSchema()
    provisionData.printSchema()
    registerRequestsData.printSchema()

    parquetWriter.storeActivityData(date.format(dateFormatter),
      activities.filter(col("date") === date.format(dateFormatter)))

    parquetWriter.storeActivityData(yesterday.format(dateFormatter),
      activities.filter(col("date") === yesterday.format(dateFormatter)))

    parquetWriter.storeProvisionData(date.format(dateFormatter), provisionData)

    parquetWriter.storeRegisterRequestsData(date.format(dateFormatter), registerRequestsData)
  }

  if (List(DataKind.PERSISTENT, DataKind.ALL).contains(runMode)) {
    val oldUserAgents = csvDataFiles.getOldUserAgents
    parquetWriter.storeOldUserAgents(oldUserAgents)
  }
}
