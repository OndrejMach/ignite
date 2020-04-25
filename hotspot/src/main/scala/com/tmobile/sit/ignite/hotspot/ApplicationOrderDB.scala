package com.tmobile.sit.ignite.hotspot

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.hotspot.config.{OrderDBConfig, Settings}
import com.tmobile.sit.ignite.hotspot.data.{OrderDBInputData, OrderDBStructures, WlanHotspotTypes}
import com.tmobile.sit.ignite.hotspot.processors.OrderDBProcessor
import com.tmobile.sit.ignite.hotspot.readers.TextReader



object ApplicationOrderDB {
  val LOAD_DATE = Timestamp.valueOf(LocalDateTime.now())
  val FUTURE = Timestamp.valueOf("4712-12-31 00:00:00")
  val ENTRY_ID = 1

  def main(args: Array[String]): Unit = {
    implicit val sparkSession = getSparkSession()

    val settings: Settings = Settings(
      OrderDBConfig(wlanHotspotFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/cptm_ta_d_wlan_hotspot.csv"),
        orderDBFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.MPS.DAY.20200408*.csv"),
        errorCodesFile = Some("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_error_code.csv")
    ))


    val orderDBProcessor = new OrderDBProcessor(orderDBInputData = OrderDBInputData(settings.orderDBFiles), maxDate = FUTURE )

    orderDBProcessor.processData()

  }


}
