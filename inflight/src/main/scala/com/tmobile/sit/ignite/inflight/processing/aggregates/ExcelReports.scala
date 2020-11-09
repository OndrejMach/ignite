package com.tmobile.sit.ignite.inflight.processing.aggregates

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.data.OutputFilters
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType}

/**
 * A very important class generationg data for Excel reports - Session report and also complete report. Reports are generated daily or monthly
 *
 * @param radiusCreditdata  - radius credit aggregates
 * @param voucherRadiusData - voucher radius aggregates
 * @param airlineCodes      - airline codes to include in the report
 * @param sparkSession      - clear
 */

class ExcelReports(radiusCreditdata: DataFrame, voucherRadiusData: DataFrame, airlineCodes: Seq[String])(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  def getSessionReport(): DataFrame = {
    logger.info("Staring processing of session report")
    val preprocessed = radiusCreditdata
      .filter(OutputFilters.filterAirline(airlineCodes))
      .withColumn("voucher_duration", col("voucher_duration") / 3600)
      .withColumn("average_session_volume", col("wlif_session_volume") / 1024)
    logger.info("Data preprocessed")
    val maxVolume = preprocessed
      .groupBy("wlif_flight_id")
      .agg(
        max("average_session_volume").alias("max_volume_on_flight")
      )
    logger.info("Max session per flight calculated")
    logger.info("Calculation final aggregations")
    //preprocessed.printSchema()

    preprocessed
      .withColumn("wlif_session_stop", $"wlif_session_stop".cast(DateType))
      .groupBy("wlif_airline_code", "wlif_flight_id",
        "wlif_session_stop", "wlif_aircraft_code",
        "wlif_flight_number", "voucher_duration",
        "wlif_airport_code_origin", "wlif_airport_code_destination")
      .agg(
        sum("average_session_volume").alias("average_session_volume"),
        countDistinct("wlif_username").alias("number_of_users"),
        sum("wlif_num_sessions").alias("wlif_num_sessions")
      )
      .withColumn("average_session_volume",
        when(col("number_of_users").gt(lit(0)), col("average_session_volume") / col("number_of_users"))
          .otherwise(lit(0)))
      .join(maxVolume, Seq("wlif_flight_id"), "left_outer")
      .withColumn("average_session_volume", round($"average_session_volume", 8))
      .withColumn("max_volume_on_flight", round($"max_volume_on_flight", 8))
      .sort("wlif_session_stop")
  }

  def getVoucherReport(): DataFrame = {
    logger.info("Starting processing of detailed reports")


    val ret = voucherRadiusData
      .filter(OutputFilters.filterAirline(airlineCodes))
      .withColumn("wlif_session_volume", round(col("wlif_session_volume") / 1024, 8))

      .withColumn("wlif_date_time_opened", $"wlif_date_time_opened".cast(DateType))
      .withColumn("wlif_date_time_closed", $"wlif_date_time_closed".cast(DateType))
      .withColumn("wlan_voucher_duration", ($"wlan_voucher_duration" / 3600).cast(LongType))
      .sort("wlif_date_time_opened")

    ret
  }

}
