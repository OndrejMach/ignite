package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.processors.udfs.TimeCalculations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SessionMetrics(quarterID: Long, volume: Double, duration: Long, start_flag: Int, end_flag: Int)

class SessionsQProcessor(dataCDRs: DataFrame, processingDate: Timestamp)(implicit sparkSession: SparkSession) extends Logger {

  private val preprocessedData = {
    import org.apache.spark.sql.functions.udf
    import sparkSession.implicits._
    val toQuartersUDF = udf(TimeCalculations.toQuarters)

    val toProcess =
      dataCDRs


    val upperDateLimit = Timestamp.valueOf(processingDate.toLocalDateTime.plusDays(-1))

    toProcess
      .withColumn("quarter_of_an_hour_id", (($"session_start_ts" - ($"session_start_ts" % 900)) % 86400) / 60)
      .withColumn("wlan_hotspot_ident_code", when($"hotspot_id".isNotNull, $"hotspot_id").otherwise(concat(lit("undefined_"), $"hotspot_owner_id")))
      .withColumn("wlan_provider_code", $"hotspot_owner_id")
      .withColumn("wlan_user_provider_code", $"user_provider_id")
      .withColumn("end_quarter", $"session_start_ts" + 900 - ($"session_start_ts" % 900))
      //.withColumn("volume_per_sec", when($"session_duration" =!= lit(0), $"session_volume".cast(DoubleType) / $"session_duration".cast(DoubleType)).otherwise(lit(0.0).cast(DoubleType)))
      .withColumn("session_start_ts", from_unixtime($"session_start_ts"-lit(2*3600))) //-lit(2*3600)
      .withColumn("session_event_ts", from_unixtime($"session_event_ts"-lit(2*3600))) //-lit(2*3600)

      .filter((col("session_event_ts") > lit(processingDate).cast(TimestampType)) )
      .withColumn("normalisedStart",
        when($"session_start_ts" < lit(processingDate).cast(TimestampType),lit(processingDate).cast(TimestampType))
          .otherwise($"session_start_ts"))
      .withColumn("quarterSplit", toQuartersUDF($"session_start_ts", $"session_event_ts", $"session_volume", $"session_duration", lit(processingDate).cast(TimestampType)))
      .withColumn("metrics", explode($"quarterSplit"))
      .drop("quarterSplit")
      .withColumn("quarter_of_an_hour_id",$"metrics".getItem("quarterID"))
      .withColumn("volume",$"metrics".getItem("volume").cast(LongType))
      .withColumn("duration", ($"metrics".getItem("duration")/lit(60)).cast(LongType))
      .withColumn("start_flag", $"metrics".getItem("start_flag"))
      .withColumn("start_flag",
        when($"session_start_ts" < lit(processingDate).cast(TimestampType),lit(0))
          .otherwise($"start_flag"))
      .withColumn("end_flag",$"metrics".getItem("end_flag") )
      .drop("metrics")
  }

  val getData: DataFrame = {
   val aggKey = Seq("wlan_session_date","quarter_of_an_hour_id",
     "wlan_user_provider_code","wlan_provider_code",
     "wlan_hotspot_ident_code","wlan_user_account_id",
     "terminate_cause_id","login_type")

    preprocessedData
      .sort(aggKey.head, aggKey.tail :_*)
      .groupBy(aggKey.head, aggKey.tail :_*)
      .agg(
        //first("terminate_cause_id").alias("terminate_cause_id"),
        sum("volume").alias("session_volume"),
        sum("duration").alias("session_duration"),
        sum("start_flag").alias("num_of_session_start"),
        sum("end_flag").alias("num_of_session_stop"),
        count("*").alias("num_of_session_active")
      )
      .withColumn("num_subscriber", lit(1))
  }

}
