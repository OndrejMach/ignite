package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.{Dataset, SparkSession}
import com.tmobile.sit.ignite.hotspot.data.OrderDBStructures
import org.apache.spark.sql.functions.{col, desc, lit}
import org.apache.spark.sql.types.LongType

class OrderDBData(orderDbReader: Reader, inputHotspot: Reader, oldErrorCodes: Reader)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  val fullData: Dataset[OrderDBStructures.OrderDBInput] = {
    logger.info("Getting OrderDB full input")
    orderDbReader
      .read()
      .filter(col("value").startsWith("D;"))
      .as[String]
      .map(Mappers.mapInputOrderDB(_))
  }

  val errorCodesDaily: Dataset[OrderDBStructures.ErrorCode] = {
    logger.info("Getting error codes")
    fullData
      .map(Mappers.mapErrorCodes(_))
  }

  val newErrorCodes = errorCodesDaily.join(
    oldErrorCodes.read().select("error_code").withColumn("old", lit(1)),
    Seq("error_code"),
    "left_outer"
  )
    .filter("old is null")


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
