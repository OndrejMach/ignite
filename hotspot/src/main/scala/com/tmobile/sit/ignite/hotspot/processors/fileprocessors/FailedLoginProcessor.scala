package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.hotspot.data.FailedLoginsStructure.FailedLogin
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * processing for Failed logins.
 * @param failedLogins - failed logins data
 * @param hotspotData - hotspot data
 * @param citiesData - cities data - this is a candidate for removal considering quality of the city data.
 * @param sparkSession
 */

class FailedLoginProcessor(failedLogins: DataFrame, hotspotData: DataFrame, citiesData: DataFrame)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  private lazy val hotspotDataForLookup = {
    logger.info("Preparing info about known hotspots for lookup")
    hotspotData
      .select("wlan_hotspot_ident_code", "wlan_hotspot_id")
    .groupBy("wlan_hotspot_ident_code")
    .agg(max("wlan_hotspot_id").alias("wlan_hotspot_id"))
  }

  private lazy val cityDataforLookup = {
logger.info("preparing cities for lookup")
    val raw = citiesData.select("city_code", "city_id")
    logger.info(s"Initialising cities lookup, count ${raw.count}")
    // bug in EVL - should be removed
    raw
      .groupBy("city_code")
      .agg(
        max("city_id").alias("city_id")
      )
  }
  val getData: DataFrame = {
    val aggKey = Seq("login_hour", "hotspot_ident_code",
      "hotspot_provider_code", "hotspot_venue_code",
      "hotspot_venue_type_code", "hotspot_city_name",
      "hotspot_country_code", "user_provider",
      "account_type_id", "login_type", "login_error_code")
    logger.info("Starting main processing block for failed logins - join with hotspot data to get info about hotspots")
    val withhotspot = failedLogins
      .join(hotspotDataForLookup, $"wlan_hotspot_ident_code" === $"hotspot_ident_code", "left_outer")
      .drop("wlan_hotspot_ident_code")
      .filter($"wlan_hotspot_id".isNotNull).persist()

    logger.info(s"With hotspot data count: ${withhotspot.count()}")
    logger.info("Joining data with cities - lookup")
    val withCity = withhotspot
      .withColumnRenamed("wlan_hotspot_id", "hotspot_id")
      .join(cityDataforLookup, $"city_code" === $"hotspot_city_name", "left_outer")
      .drop("city_code")
      .filter($"city_id".isNotNull).persist()

    logger.info(s"With City data count: ${withCity.count()}")

    //$"withCity.printSchema()
    logger.info("Doing final aggregations")
    withCity
      .sort("login_datetime", aggKey: _*)
      .groupBy(aggKey.head, aggKey.tail: _*)
      .agg(
        count("*").alias("num_of_failed_logins"),
        last("login_datetime").alias("login_datetime"),
        last("login_date").alias("login_date"),
        first("hotspot_id").alias("hotspot_id"),
        first("city_id").alias("city_id")
      )
      .withColumnRenamed("hotspot_city_name", "city_name")
      .na.fill("UNDEFINED", Seq("user_provider"))
  }

}
