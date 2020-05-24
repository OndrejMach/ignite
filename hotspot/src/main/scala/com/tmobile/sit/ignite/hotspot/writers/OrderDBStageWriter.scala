package com.tmobile.sit.ignite.hotspot.writers

import com.tmobile.sit.common.writers.Writer
import com.tmobile.sit.ignite.hotspot.data.StageStructures
import com.tmobile.sit.ignite.hotspot.processors.staging.OderdDBPRocessingOutputs
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class OrderDBStageFilenames(
                                  wlanHotspot: String ,
                                  errorCodes: String ,
                                  mapVoucher: String,
                                  orderDb: String )


class OrderDBStageWriter(data: OderdDBPRocessingOutputs, filenames: OrderDBStageFilenames) (implicit sparkSession: SparkSession) extends Writer {
  override def writeData(): Unit = {

    def writeFilePartitioned(path: String, data: DataFrame, saveMode: SaveMode): Unit = {
      logger.info(s"Writing partitioned file to ${path}")
      val tmpPath = path+"_tmp"
      data
        .repartition(1)
        .write
        .mode(saveMode)
        .partitionBy("year", "month", "day")
        .parquet(tmpPath)

      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      fs.rename(new Path(tmpPath), new Path(path))
    }
    def writeFile(path: String, data: DataFrame, saveMode: SaveMode): Unit = {
      logger.info(s"Writing file unpartitioned ${path}")
      val tmpPath = path+"_tmp"
      data
        .repartition(1)
        .write
        .mode(saveMode)
        .parquet(tmpPath)

      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      fs.rename(new Path(tmpPath), new Path(path))
    }
    logger.info("Writing new Error codes")
    writeFile( filenames.errorCodes,data.errorCodes.select(StageStructures.ERROR_CODES.head, StageStructures.ERROR_CODES.tail :_*), SaveMode.Overwrite)
    logger.info("Writing new Map Voucher")
    writeFilePartitioned(filenames.mapVoucher, data.mapVoucher.select(StageStructures.MAP_VOUCHER.head,StageStructures.MAP_VOUCHER.tail :_*), SaveMode.Append)
    logger.info("Writing new OrderDB")
    writeFilePartitioned(filenames.orderDb, data.orderDb.select(StageStructures.ORDER_DB.head, StageStructures.ORDER_DB.tail :_*), SaveMode.Append)
    logger.info("Writing new wlan hotspot")
    writeFile(filenames.wlanHotspot, data.wlanHotspot.select(StageStructures.WLAN_HOTSPOT.head, StageStructures.WLAN_HOTSPOT.tail :_*),SaveMode.Overwrite)
  }
}
