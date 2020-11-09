package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.processors.udfs.TimeCalculations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.strictfp

/**
 * supportin case class holding values for each 15-minuts split
 */
case class SessionMetrics(quarterID: Long, volume: Double, duration: Long, start_flag: Int, end_flag: Int)

/**
 * This class calculates SessionQ data which is basically split of all active sessions into 15-minutes chunks. Metrics such as duration of volume are interpolated based on theoretical throuput per second.
 * These junks are then aggregated based on certain criteria and returned as a result.
 * @param dataCDRs - input CDRs
 * @param processingDate - date for processing
 * @param sparkSession
 */
@strictfp
class SessionsQProcessor(dataCDRs: DataFrame, processingDate: Timestamp)(implicit sparkSession: SparkSession) extends Logger {

  private val preprocessedData = {
    import org.apache.spark.sql.functions.udf
    import sparkSession.implicits._
    logger.info("preparing input data - technical columns, data types transormation and split into 15 minutes chunks")
    val toQuartersUDF = udf(TimeCalculations.toQuartersUnixTime)

    val inter = dataCDRs
      .withColumn("wlan_hotspot_ident_code", when($"hotspot_id".isNotNull, $"hotspot_id").otherwise(concat(lit("undefined_"), $"hotspot_owner_id")))
      .withColumn("wlan_provider_code", $"hotspot_owner_id")
      .withColumn("wlan_user_provider_code", $"user_provider_id")
      .withColumn("startTS", $"session_start_ts")
      .withColumn("eventTS", $"session_event_ts")
      .withColumn("session_start_ts", from_unixtime($"session_start_ts" - lit(2 * 3600))) //-lit(2*3600)
      .withColumn("session_event_ts", from_unixtime($"session_event_ts" - lit(2 * 3600))) //-lit(2*3600)



    inter
      .withColumn("quarterSplit", toQuartersUDF($"startTS", $"eventTS", $"session_volume", $"session_duration", lit(processingDate).cast(TimestampType)))
      .withColumn("metrics", explode($"quarterSplit"))
      .drop("quarterSplit")
      .withColumn("quarter_of_an_hour_id", $"metrics".getItem("quarterID"))
      .withColumn("volume", $"metrics".getItem("volume")) //.cast(LongType))
      .withColumn("duration", $"metrics".getItem("duration").cast(LongType)) //was /60
      .withColumn("start_flag", $"metrics".getItem("start_flag"))
      .withColumn("end_flag", $"metrics".getItem("end_flag"))
      .na.fill(0.0, Seq("volume"))
      .drop("metrics")
  }

  val getData: DataFrame = {
   import sparkSession.implicits._
    val aggKey = Seq("wlan_session_date", "quarter_of_an_hour_id",
      "wlan_user_provider_code", "wlan_provider_code",
      "wlan_hotspot_ident_code", "wlan_user_account_id",
      "terminate_cause_id", "login_type")
    logger.info("Doing final aggregation.")
    preprocessedData
      .sort(aggKey.head, aggKey.tail: _*)
      .groupBy(aggKey.head, aggKey.tail: _*)
      .agg(
        //first("terminate_cause_id").alias("terminate_cause_id"),
        sum($"volume".cast(LongType)).alias("session_volume"),
        sum($"volume").alias("double_session_volume"),
        sum("duration").alias("session_duration"),
        sum("start_flag").alias("num_of_session_start"),
        sum("end_flag").alias("num_of_session_stop"),
        count("*").alias("num_of_session_active")
      )
      .withColumn("num_subscriber", lit(1))
  }

}
