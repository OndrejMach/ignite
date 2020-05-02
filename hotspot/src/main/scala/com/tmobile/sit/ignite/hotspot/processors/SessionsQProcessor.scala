package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SessionMetrics(quarterID: Long, volume: Double, duration: Long)

class SessionsQProcessor(dataCDRsActual: DataFrame, dataCDRsminus1Day: DataFrame, dataCDRsplus1Day: DataFrame, processingDate: Timestamp)(implicit sparkSession: SparkSession) extends Logger {

  val toQuarters = (a: Timestamp, e: Timestamp, volumeRatio: Double) => {
    val endDate = if (e.toLocalDateTime.getDayOfMonth != a.toLocalDateTime.getDayOfMonth) {
      val plus1Day = a.toLocalDateTime.plusDays(1)
      LocalDateTime.of(plus1Day.getYear, plus1Day.getMonth, plus1Day.getDayOfMonth, 0, 0, 0, 0)
    } else {
      e.toLocalDateTime
    }

    def getNextQuarter(d: LocalDateTime): LocalDateTime = {
      val nextQuarter = d.plusMinutes(15 - (d.getMinute % 15))
      LocalDateTime.of(nextQuarter.getYear, nextQuarter.getMonth, nextQuarter.getDayOfMonth, nextQuarter.getHour, nextQuarter.getMinute, 0, 0)
    }

    def getQuarterID(d: LocalDateTime): Int = {
      d.getMinute / 15
    }

    def durationBetween(a: LocalDateTime) : Long = {
      println(s"a: ${a.getYear}-${a.getMonth}-${a.getDayOfMonth} e:${endDate.getYear}-${endDate.getMonth}-${endDate.getDayOfMonth}")
      if (((a.getMinute / 15) == (endDate.getMinute / 15)) && (ChronoUnit.MINUTES.between(a, endDate) <= 15)) ChronoUnit.SECONDS.between(a, endDate)
      else {
        val next = getNextQuarter(a)
        ChronoUnit.MINUTES.between(a, next)*60
      }
    }

    for {i <- 0 to (ChronoUnit.MINUTES.between(a.toLocalDateTime, endDate).toInt / 15)} yield {
      if (i == 0) {
        val duration = durationBetween(a.toLocalDateTime)
        SessionMetrics(
          quarterID = getQuarterID(a.toLocalDateTime),
          duration = duration,
          volume = volumeRatio * duration
        )
      } else {
        val duration = durationBetween(getNextQuarter(a.toLocalDateTime.plusMinutes((i - 1) * 15)))
        SessionMetrics(
          quarterID = getQuarterID(a.toLocalDateTime.plusMinutes(i * 15)),
          duration = duration,
          volume = volumeRatio * duration
        )
      }
    }
  }


  val getData: DataFrame = {
    import org.apache.spark.sql.functions.udf
    import sparkSession.implicits._
    val toQuartersUDF = udf(toQuarters)

    val toProcess =
      dataCDRsActual
    // .union(dataCDRsminus1Day)
    // .union(dataCDRsplus1Day)

    val upperDateLimit = Timestamp.valueOf(processingDate.toLocalDateTime.plusDays(1))

    toProcess
      .withColumn("quarter_of_an_hour_id", (($"session_start_ts" - ($"session_start_ts" % 900)) % 86400) / 60)
      .withColumn("wlan_hotspot_ident_code", when($"hotspot_id".isNotNull, $"hotspot_id").otherwise(concat(lit("undefined_"), $"hotspot_owner_id")))
      .withColumn("wlan_provider_code", $"hotspot_owner_id")
      .withColumn("wlan_user_provider_code", $"user_provider_id")
      .withColumn("end_quarter", $"session_start_ts" + 900 - ($"session_start_ts" % 900))
      .withColumn("volume_per_sec", when($"session_duration" =!= lit(0), $"session_volume".cast(DoubleType) / $"session_duration").otherwise(lit(0.0)))
      .withColumn("session_start_ts", from_unixtime($"session_start_ts"))
      .withColumn("session_event_ts", from_unixtime($"session_event_ts"))
      .filter(!((col("session_start_ts") >= lit(upperDateLimit)) || (col("session_event_ts") <= lit(processingDate))))
      .withColumn("quarterSplit", toQuartersUDF($"session_start_ts", $"session_event_ts", $"volume_per_sec"))
  }

}
