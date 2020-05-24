package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.hotspot.config.{InputConfig, StageConfig}
import com.tmobile.sit.ignite.hotspot.readers.TextReader
import org.apache.spark.sql.SparkSession

class OrderDBInputData(stageConfig: StageConfig, inputConfig: InputConfig)(implicit  sparkSession: SparkSession) {
  val dataHotspotReader = CSVReader(path = stageConfig.wlan_hotspot_filename.get,
    header = false,
    delimiter = "~",
    schema = Some(WlanHotspotTypes.wlanHotspotStructure),
    quote = ""
  )

  val inputMPSReader = new TextReader(inputConfig.CDR_filename.get)

  val oldErrorCodesReader =
    CSVReader(schema = Some(OrderDBStructures.errorCodesStructure),
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      path = stageConfig.error_codes_filename.get,
      header = false,
      delimiter = "|")
}

object OrderDBInputData {
  def apply(stageConfig: StageConfig, inputConfig: InputConfig)(implicit sparkSession: SparkSession): OrderDBInputData = new OrderDBInputData(stageConfig, inputConfig: InputConfig)
}