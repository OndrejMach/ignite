package com.tmobile.sit.ignite.hotspot.writers

import com.tmobile.sit.ignite.common.common.writers.{CSVWriter, Writer}
import com.tmobile.sit.ignite.hotspot.data.StageStructures
import com.tmobile.sit.ignite.hotspot.processors.staging.OderdDBPRocessingOutputs
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


case class OrderDBStageFilenames(
                                  wlanHotspot: String ,
                                  errorCodes: String ,
                                  mapVoucher: String,
                                  orderDb: String )

/**
 * Writing data related to OrderDB structures.
 * @param data
 * @param filenames
 * @param sparkSession
 */


class OrderDBStageWriter(data: OderdDBPRocessingOutputs, filenames: OrderDBStageFilenames) (implicit sparkSession: SparkSession) extends Writer {
  import sparkSession.implicits._

  override def writeData(): Unit = {

    def writeFilePartitioned(path: String, data: DataFrame, saveMode: SaveMode): Unit = {
      val toWrite = data.repartition()
      logger.info(s"Writing partitioned file to ${path} data count: ${toWrite.count()}")
      toWrite
        .repartition(1)
        .write
        .mode(saveMode)
        .partitionBy("year", "month", "day")
        .parquet(path)

    }

    def writeData(data: DataFrame, filename: String) = {
      val toWrite = data.cache()
      logger.info(s"Writing data to ${filename}, count: ${toWrite.count()}")
      toWrite
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(filename)
    }

    logger.info("Writing new Error codes")
    CSVWriter(
      path = filenames.errorCodes,
      writeHeader = true,
      data= data.errorCodes.select(StageStructures.ERROR_CODES.head, StageStructures.ERROR_CODES.tail :_*),
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      dateFormat = "yyyy-MM-dd HH:mm:ss",
      quote = "\u0000"
    )
    //writeFile( filenames.errorCodes,data.errorCodes.select(StageStructures.ERROR_CODES.head, StageStructures.ERROR_CODES.tail :_*), SaveMode.Overwrite)
    logger.info("Writing new Map Voucher")
    writeFilePartitioned(filenames.mapVoucher,
      data
        .mapVoucher
        .select(StageStructures.MAP_VOUCHER.head,StageStructures.MAP_VOUCHER.tail :_*)
        .withColumn("wlan_request_date", date_format($"wlan_request_date","yyyy-MM-dd HH:mm:ss"))
      ,
      SaveMode.Append)
    logger.info("Writing new OrderDB")
    writeFilePartitioned(filenames.orderDb, data.orderDb.select(StageStructures.ORDER_DB.head, StageStructures.ORDER_DB.tail :_*), SaveMode.Append)

    logger.info("Writing new wlan hotspot")
    writeData(
      data = data.wlanHotspot.select(StageStructures.WLAN_HOTSPOT.head, StageStructures.WLAN_HOTSPOT.tail :_*),
      filename= filenames.wlanHotspot
    )

  }
}
