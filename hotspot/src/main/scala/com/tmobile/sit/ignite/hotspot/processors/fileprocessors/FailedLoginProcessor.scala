package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.data.FailedLoginsStructure.FailedLogin
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * processing for Failed logins.
 * @param failedLogins - failed logins data
 * @param hotspotData - hotspot data
 * @param citiesData - cities data - this is a candidate for removal considering quality of the city data.
 * @param errorCodes - error codes :)
 * @param sparkSession
 */

class FailedLoginProcessor(failedLogins: DataFrame, hotspotData: DataFrame, citiesData: DataFrame, errorCodes: DataFrame)(implicit sparkSession: SparkSession) extends Logger {

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

  private lazy val errorCodesLookup = {
    logger.info("preparing error codes lookup")
    errorCodes.select("error_desc", "error_id")
  }


  private lazy val rawData = {
    logger.info("Reading failed logins raw data")
    val ret = failedLogins
      //.read()
      .filter($"value".startsWith("D;"))
      .as[String]
      .map(i => FailedLogin(i)).toDF()
    logger.info(s"rawDataCount = ${ret.count()}") //0562862139
    ret
  }

  private lazy val preprocessedData = {
   logger.info("preprocessing input data")
    def fixEmptyString(columnName: String) = when(trim(col(columnName)).equalTo(""), lit("UNDEFINED")).otherwise(trim(col(columnName)))

    val ret = rawData
      .withColumn("login_attempt_ts", $"login_attempt_ts" - lit(2*3600))
      .withColumn("login_datetime", when($"hotspot_provider_code".equalTo(lit("TMUK")), from_unixtime($"login_attempt_ts" - lit(3600))).otherwise(from_unixtime($"login_attempt_ts")))
      .withColumn("login_date", $"login_datetime".cast(DateType))
      .withColumn("login_hour", date_format($"login_datetime", "yyyyMMddHH"))
      .na.fill("UNDEFINED", Seq("hotspot_country_code", "user_provider", "hotspot_ident_code", "hotspot_provider_code", "hotspot_venue_code", "hotspot_venue_type_code", "hotspot_city_name"))
      .withColumn("hotspot_country_code",fixEmptyString("hotspot_country_code"))
      .withColumn("user_provider",fixEmptyString("user_provider"))
      .withColumn("hotspot_ident_code",fixEmptyString("hotspot_ident_code"))
      .withColumn("hotspot_provider_code",fixEmptyString("hotspot_provider_code"))
      .withColumn("hotspot_venue_code",fixEmptyString("hotspot_venue_code"))
      .withColumn("hotspot_venue_type_code",fixEmptyString("hotspot_venue_type_code"))
      .withColumn("hotspot_city_name",fixEmptyString("hotspot_city_name"))
      .withColumn("hotspot_city_name", when($"hotspot_city_name".equalTo(lit("*")), "UNDEFINED").otherwise($"hotspot_city_name"))
      .withColumnRenamed("login_id", "tid")
      .join(errorCodesLookup, $"login_error_code" === $"error_desc", "left_outer")
      .drop("error_desc")
      .drop("login_error_code")
      .withColumnRenamed("error_id", "login_error_code")

    logger.info(s"Preprocessed data count: ${ret.count()}")
    ret
  }

  val getData: DataFrame = {
    val aggKey = Seq("login_hour", "hotspot_ident_code",
      "hotspot_provider_code", "hotspot_venue_code",
      "hotspot_venue_type_code", "hotspot_city_name",
      "hotspot_country_code", "user_provider",
      "account_type_id", "login_type", "login_error_code")
    logger.info("Starting main processing block for failed logins - join with hotspot data to get info about hotspots")
    val withhotspot = preprocessedData
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
