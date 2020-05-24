package com.tmobile.sit.ignite.hotspot.writers

import java.time.format.DateTimeFormatter

import com.tmobile.sit.common.writers.{CSVWriter, Writer}
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.OutputStructures
import com.tmobile.sit.ignite.hotspot.processors.StageData
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class StageFilesWriter(stageData: StageData)(implicit sparkSession: SparkSession, settings: Settings) extends Writer {

  private def writeParitioned(dateColumn: String, data: DataFrame, filename: String) = {
    logger.info("Writing partitioned parquet")
    val tmpPath = filename+"_tmp"
    val add = data
      .withColumn("year", year(col(dateColumn)))
      .withColumn("month", month(col(dateColumn)))
      .withColumn("day", dayofmonth(col(dateColumn)))
      .repartition(1)

      add.printSchema()

    add.write
      .mode(SaveMode.Append)
      .partitionBy("year","month","day")
      .parquet(filename)
   // handleTmp(tmpPath, filename)
  }

  private def writePartitionedDate(data: DataFrame, filename: String) = {
    val processingDate = settings.appConfig.processing_date.get.toLocalDateTime
    val procDateString = processingDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    logger.info(s"Writing data partitioned by processing date for ${procDateString}")
    val tmpPath = filename+"_tmp"
    data
      .withColumn("date", lit(procDateString))
      .repartition(1)
      .write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .parquet(filename)
   //handleTmp(tmpPath, filename)

  }


  def writeData() = {

    logger.info(s"Writing SessionD file to ${settings.stageConfig.session_d.get}")
    writeParitioned("wlan_session_date",
      stageData
        .sessionD
        .select(OutputStructures.SESSION_D_OUTPUT_COLUMNS.head, OutputStructures.SESSION_D_OUTPUT_COLUMNS.tail: _*),
      settings.stageConfig.session_d.get
    )

    logger.info(s"Writing cities file to ${settings.stageConfig.city_data.get}")
    CSVWriter(path = settings.stageConfig.city_data.get, //"/Users/ondrejmachacek/tmp/hotspot/out/cities.csv",
      delimiter = "|",
      writeHeader = true,
      data = stageData.cities.select(OutputStructures.CITIES_OUTPUT_COLUMNS.head, OutputStructures.CITIES_OUTPUT_COLUMNS.tail: _*))
      .writeData()

    val vchrs = stageData.vouchers
      .select(OutputStructures.VOUCHER_OUTPUT_COLUMNS.head, OutputStructures.VOUCHER_OUTPUT_COLUMNS.tail: _*)
      .repartition(1)
    logger.info(s"Writing vouchers to ${settings.stageConfig.wlan_voucher.get} (data count: ${vchrs.count()})")
    vchrs
      .write
      .mode(SaveMode.Overwrite)
      .parquet(settings.stageConfig.wlan_voucher.get+"_tmp")

    logger.info(s"Writing failed transactions to ${settings.stageConfig.failed_transactions.get}")
    writePartitionedDate(data = stageData.failedTransactions
      .select(OutputStructures.FAILED_TRANSACTIONS_COLUMNS.head, OutputStructures.FAILED_TRANSACTIONS_COLUMNS.tail: _*),
      filename = settings.stageConfig.failed_transactions.get
    )

    logger.info(s"Writing orderDB_H to ${settings.stageConfig.orderDB_H.get}")
    writePartitionedDate(
      data = stageData.orderDBH.select(OutputStructures.ORDERDB_H_COLUMNS.head, OutputStructures.ORDERDB_H_COLUMNS.tail: _*),
      filename = settings.stageConfig.orderDB_H.get
    )

    logger.info(s"Writing Session_Q to ${settings.stageConfig.session_q.get}")
    writePartitionedDate(
      data = stageData.sessionQ.select(OutputStructures.SESSION_Q_COLUMNS.head, OutputStructures.SESSION_Q_COLUMNS.tail: _*),
      filename = settings.stageConfig.session_q.get
    )

    logger.info(s"Writing failed logins to ${settings.stageConfig.failed_logins.get}")
    writeParitioned(
      dateColumn = "login_datetime",
      data = stageData.failedLogins
        .select(OutputStructures.FAILED_LOGINS_OUTPUT_COLUMNS.head, OutputStructures.FAILED_LOGINS_OUTPUT_COLUMNS.tail: _*),
      filename = settings.stageConfig.failed_logins.get
    )
  }

}
