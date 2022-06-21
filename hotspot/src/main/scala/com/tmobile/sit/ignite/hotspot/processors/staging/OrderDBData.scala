package com.tmobile.sit.ignite.hotspot.processors.staging

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.Reader
import com.tmobile.sit.ignite.hotspot.data.OrderDBStructures
import com.tmobile.sit.ignite.hotspot.data.OrderDBStructures.{ErrorCode, OrderDBInput}
import org.apache.spark.sql.functions.{col, desc, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * wrapper class for orderDB supporting data
 * @param orderDb - orderDB data
 * @param inputHotspot - input hotspot data
 * @param oldErrorCodes - error codes data
 * @param sparkSession
 */

class OrderDBData(orderDb: DataFrame, inputHotspot: DataFrame, oldErrorCodes: DataFrame)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  lazy val allOldErrorCodes = oldErrorCodes

  lazy val fullData: Dataset[OrderDBStructures.OrderDBInput] = {
    logger.info("Getting OrderDB full input")
    orderDb
      .filter(col("value").startsWith("D;"))
      .as[String]
      .map(OrderDBInput(_))
  }

  lazy val errorCodesDaily: Dataset[OrderDBStructures.ErrorCode] = {
    logger.info("Getting error codes")
    fullData
      .map(ErrorCode(_))
  }

  lazy val newErrorCodes = errorCodesDaily.join(
    allOldErrorCodes.select("error_code").withColumn("old", lit(1)),
    Seq("error_code"),
    "left_outer"
  )
    .filter("old is null")
    .drop("old")


  lazy val hotspotFile = {
    logger.info("Reading old hotspot file (cptm_ta_d_wlan_hotspot)")
    inputHotspot
  }

  lazy val hotspotIDsSorted = {
    logger.info("Getting hotspots sorted")
    hotspotFile
      .select(col("wlan_hotspot_id")
        .cast(LongType))
      .sort(desc("wlan_hotspot_id"))
      .distinct()
  }

}
