package com.tmobile.sit.ignite.hotspot.processors.staging

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.Reader
import com.tmobile.sit.ignite.hotspot.data.OrderDBStructures
import com.tmobile.sit.ignite.hotspot.data.OrderDBStructures.{ErrorCode, OrderDBInput}
import org.apache.spark.sql.functions.{col, desc, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SparkSession}

class OrderDBData(orderDbReader: Reader, inputHotspot: Reader, oldErrorCodes: Reader)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  val allOldErrorCodes = oldErrorCodes.read()

  val fullData: Dataset[OrderDBStructures.OrderDBInput] = {
    logger.info("Getting OrderDB full input")
    orderDbReader
      .read()
      .filter(col("value").startsWith("D;"))
      .as[String]
      .map(OrderDBInput(_))
  }

  val errorCodesDaily: Dataset[OrderDBStructures.ErrorCode] = {
    logger.info("Getting error codes")
    fullData
      .map(ErrorCode(_))
  }

  val newErrorCodes = errorCodesDaily.join(
    allOldErrorCodes.select("error_code").withColumn("old", lit(1)),
    Seq("error_code"),
    "left_outer"
  )
    .filter("old is null")
    .drop("old")


  val hotspotFile = {
    logger.info("Reading old hotspot file (cptm_ta_d_wlan_hotspot)")
    inputHotspot.read()
  }

  val hotspotIDsSorted = {
    logger.info("Getting hotspots sorted")
    hotspotFile
      .select(col("wlan_hotspot_id")
        .cast(LongType))
      .sort(desc("wlan_hotspot_id"))
      .distinct()
  }

}
