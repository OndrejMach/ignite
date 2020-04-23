package com.tmobile.sit.ignite.hotspot

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.hotspot.data.{OrderDBStructures, WlanHotspotTypes}
import com.tmobile.sit.ignite.hotspot.processors.OrderDBProcessor
import com.tmobile.sit.ignite.hotspot.readers.TextReader



object ApplicationOrderDB {
  val LOAD_DATE = Timestamp.valueOf(LocalDateTime.now())
  val FUTURE = Timestamp.valueOf("4712-12-31 00:00:00")
  val ENTRY_ID = 1



  def main(args: Array[String]): Unit = {
    implicit val sparkSession = getSparkSession()





    val dataHotspotReader = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/cptm_ta_d_wlan_hotspot.csv", header = false, delimiter = "~", schema = Some(WlanHotspotTypes.wlanHotspotStructure))

    val inputMPSReader = new TextReader("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.MPS.DAY.20200408*.csv")
    val oldErrorCodesReader =
      CSVReader(schema = Some(OrderDBStructures.errorCodesStructure),
        timestampFormat = "yyyy-MM-dd HH:mm:ss",
        path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_error_code.csv",
        header = false,
        delimiter = "|")

    val orderDBProcessor = new OrderDBProcessor(inputHotspot = dataHotspotReader,inputMPS = inputMPSReader, inputErrorCodes = oldErrorCodesReader, maxDate = FUTURE )

    orderDBProcessor.processData()

    //dataHotspotReader.read().show(false)





    //TBL_NAME=orderdb
    //TBL_PREF=cptm_ta_f_wlan

    // "D;79746013287330544778;20200407;090724;;MPSI;;4.95;EUR;KO;;MOBILE_APERTO;19.0;mobile_aperto;PASS;DE/TCOM/ForFreePass_30min_Venue/DB_Bahnhoefe/Freiburg_Brsg./761138399700/XYZ;TMD;;lukasmax57@yahoo.com;86400;;;;;;;;;;;".split(";").foreach(println(_))


  }


}
