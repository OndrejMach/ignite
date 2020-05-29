package com.tmobile.sit.ignite.hotspot.writers

import java.time.format.DateTimeFormatter

import com.tmobile.sit.common.writers.{CSVWriter, Writer}
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.StageStructures
import com.tmobile.sit.ignite.hotspot.processors.StageData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class StageFilesWriter(stageData: StageData)(implicit sparkSession: SparkSession, settings: Settings) extends Writer {

  private def writeParquet(data: DataFrame, filename: String) = {
    val toWrite = data.cache()
    logger.info(s"Writing partitioned parquet to ${filename} data count: ${toWrite.count()}")
    toWrite
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(filename)
    // handleTmp(tmpPath, filename)
  }

  private def writeParitionedByProcessingDate(data: DataFrame, filename: String) = {
    logger.info(s"Writing partitioned parquet to ${filename} data count: ${data.count()}")
    data
      .withColumn("date", lit(settings.appConfig.processing_date.get.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      .repartition(1)
      .write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .parquet(filename)
    // handleTmp(tmpPath, filename)
  }


  def writeData() = {
    import sparkSession.implicits._

    logger.info(s"Writing SessionD file to ${settings.stageConfig.session_d.get}")
    writeParitionedByProcessingDate(
      stageData
        .sessionD
        .select(StageStructures.SESSION_D_OUTPUT_COLUMNS.head, StageStructures.SESSION_D_OUTPUT_COLUMNS.tail: _*),
      settings.stageConfig.session_d.get
    )

    logger.info(s"Writing cities file to ${settings.stageConfig.city_data.get}")
    CSVWriter(path = settings.stageConfig.city_data.get, //"/Users/ondrejmachacek/tmp/hotspot/out/cities.csv",
      delimiter = "|",
      writeHeader = true,
      data = stageData.cities.select(StageStructures.CITIES_OUTPUT_COLUMNS.head, StageStructures.CITIES_OUTPUT_COLUMNS.tail: _*))
      .writeData()


    logger.info(s"Writing vouchers to ${settings.stageConfig.wlan_voucher.get}")
    CSVWriter(
      path = settings.stageConfig.wlan_voucher.get,
      delimiter = "|",
      writeHeader = true,
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      dateFormat = "yyyy-MM-dd",
      data = stageData.vouchers
        .select(StageStructures.VOUCHER_OUTPUT_COLUMNS.head, StageStructures.VOUCHER_OUTPUT_COLUMNS.tail: _*)
    ).writeData()

    logger.info(s"Writing failed transactions to ${settings.stageConfig.failed_transactions.get}")
    writeParitionedByProcessingDate(data = stageData.failedTransactions
      .select(StageStructures.FAILED_TRANSACTIONS_COLUMNS.head, StageStructures.FAILED_TRANSACTIONS_COLUMNS.tail: _*),
      filename = settings.stageConfig.failed_transactions.get
    )

    logger.info(s"Writing orderDB_H to ${settings.stageConfig.orderDB_H.get}")
    writeParitionedByProcessingDate(
      data = stageData.orderDBH.select(StageStructures.ORDERDB_H_COLUMNS.head, StageStructures.ORDERDB_H_COLUMNS.tail: _*),
      filename = settings.stageConfig.orderDB_H.get
    )

    logger.info(s"Writing Session_Q to ${settings.stageConfig.session_q.get}")
    writeParitionedByProcessingDate(
      data = stageData.sessionQ.select(StageStructures.SESSION_Q_COLUMNS.head, StageStructures.SESSION_Q_COLUMNS.tail: _*),
      filename = settings.stageConfig.session_q.get
    )

    logger.info(s"Writing failed logins to ${settings.stageConfig.failed_logins.get}")
    stageData.failedLogins
      .select(StageStructures.FAILED_LOGINS_OUTPUT_COLUMNS.head, StageStructures.FAILED_LOGINS_OUTPUT_COLUMNS.tail: _*)
      .withColumn("year", year($"login_datetime"))
      .withColumn("month", month($"login_datetime"))
      .withColumn("day", dayofmonth($"login_datetime"))
      .write
      .mode(SaveMode.Append)
      .parquet(settings.stageConfig.failed_logins.get)


    logger.info(s"Writing new hotspot data")
    writeParquet(data = stageData.hotspotNew, filename = settings.stageConfig.wlan_hotspot_filename.get)
  }

}
