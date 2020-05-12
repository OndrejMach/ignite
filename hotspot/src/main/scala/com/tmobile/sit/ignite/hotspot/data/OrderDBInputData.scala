package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.hotspot.config.OrderDBConfig
import com.tmobile.sit.ignite.hotspot.readers.TextReader
import org.apache.spark.sql.SparkSession

class OrderDBInputData(orderDBConfig: OrderDBConfig)(implicit  sparkSession: SparkSession) {
  val dataHotspotReader = CSVReader(path = orderDBConfig.wlanHotspotFile.get,
    header = false,
    delimiter = "~",
    schema = Some(WlanHotspotTypes.wlanHotspotStructure),
    quote = ""
  )

  val inputMPSReader = new TextReader(orderDBConfig.orderDBFile.get)

  val oldErrorCodesReader =
    CSVReader(schema = Some(OrderDBStructures.errorCodesStructure),
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      path = orderDBConfig.errorCodesFile.get,
      header = false,
      delimiter = "|")
}

object OrderDBInputData {
  def apply(orderDBConfig: OrderDBConfig)(implicit sparkSession: SparkSession): OrderDBInputData = new OrderDBInputData(orderDBConfig)
}