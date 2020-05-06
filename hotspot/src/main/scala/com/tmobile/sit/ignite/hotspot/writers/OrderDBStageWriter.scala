package com.tmobile.sit.ignite.hotspot.writers

import com.tmobile.sit.common.writers.Writer
import com.tmobile.sit.ignite.hotspot.data.StageStructures
import com.tmobile.sit.ignite.hotspot.processors.staging.OderdDBPRocessingOutputs
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class OrderDBStageFilenames(
                                  wlanHotspot: String = "/Users/ondrejmachacek/tmp/hotspot/cptm_ta_d_wlan_hotspot",
                                  errorCodes: String = "/Users/ondrejmachacek/tmp/hotspot/cptm_ta_d_wlan_error_code",
                                  mapVoucher: String = "/Users/ondrejmachacek/tmp/hotspot/cptm_ta_f_wlif_map_voucher",
                                  orderDb: String = "/Users/ondrejmachacek/tmp/hotspot/cptm_ta_f_wlan_orderdb")


class OrderDBStageWriter(data: OderdDBPRocessingOutputs, filenames: OrderDBStageFilenames) (implicit sparkSession: SparkSession) extends Writer {
  override def writeData(): Unit = {
    def writeFilePartitioned(path: String, data: DataFrame, saveMode: SaveMode): Unit = {
      data
        .repartition(1)
        .write
        .mode(saveMode)
        .partitionBy("year", "month", "day")
        .parquet(path)
    }
    def writeFile(path: String, data: DataFrame, saveMode: SaveMode): Unit = {
      data
        .repartition(1)
        .write
        .mode(saveMode)
        .parquet(path)
    }
    writeFile( filenames.errorCodes,data.errorCodes.select(StageStructures.ERROR_CODES.head, StageStructures.ERROR_CODES.tail :_*), SaveMode.Overwrite)
    writeFilePartitioned(filenames.mapVoucher, data.mapVoucher.select(StageStructures.MAP_VOUCHER.head,StageStructures.MAP_VOUCHER.tail :_*), SaveMode.Append)
    writeFilePartitioned(filenames.orderDb, data.orderDb.select(StageStructures.ORDER_DB.head, StageStructures.ORDER_DB.tail :_*), SaveMode.Append)
    writeFile(filenames.wlanHotspot, data.wlanHotspot.select(StageStructures.WLAN_HOTSPOT.head, StageStructures.WLAN_HOTSPOT.tail :_*),SaveMode.Overwrite)
  }
}
