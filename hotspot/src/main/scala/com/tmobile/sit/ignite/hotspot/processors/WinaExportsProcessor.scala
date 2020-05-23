package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.functions.{date_add, datediff, dayofweek, lit, max, sum, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType}

class WinaExportsProcessor(sessionDData: DataFrame)(implicit sparkSession: SparkSession) {

  import sparkSession.implicits._

  private val winaColumns = Seq("session_day_diff", "hotspot_ident_code", "session_volume", "num_of_stop_tickets")


  private def getSessionDayDiff(sessionDate: Column) = {
    val today = Date.valueOf(LocalDate.of(2020, 5, 17))

    when(sessionDate === lit(1), datediff(lit(today).cast(DateType), date_add($"wlan_session_date", -5)))
      .otherwise(when(sessionDate === lit(2), datediff(lit(today).cast(DateType), date_add($"wlan_session_date", -6)))
        .otherwise(when(sessionDate === lit(3), datediff(lit(today).cast(DateType), $"wlan_session_date"))
          .otherwise(when(sessionDate === lit(4), datediff(lit(today).cast(DateType), date_add($"wlan_session_date", -1)))
            .otherwise(when(sessionDate === lit(5), datediff(lit(today).cast(DateType), date_add($"wlan_session_date", -2)))
              .otherwise(when(sessionDate === lit(6), datediff(lit(today).cast(DateType), date_add($"wlan_session_date", -3)))
                .otherwise(when(sessionDate === lit(0), datediff(lit(today).cast(DateType), date_add($"wlan_session_date", -4))))
              )
            )
          )
        )
      )
  }

  private def aggregateWina(data: DataFrame) = {
    data
      .withColumn("sessions_last_3_months", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(90)), $"num_of_stop_tickets").otherwise(lit(null).cast(LongType)))
      .withColumn("volume_last_3_months", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(90)), $"session_volume").otherwise(lit(null).cast(LongType)))
      .withColumn("is_3_months", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(90)), lit(1)).otherwise(lit(0).cast(LongType)))
      .withColumn("sessions_last_month", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(34)), $"num_of_stop_tickets").otherwise(lit(null).cast(LongType)))
      .withColumn("volume_last_month", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(34)), $"session_volume").otherwise(lit(null).cast(LongType)))
      .withColumn("is_month", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(34)), lit(1)).otherwise(lit(0).cast(LongType)))
      .withColumn("sessions_last_week", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(13)), $"num_of_stop_tickets").otherwise(lit(null).cast(LongType)))
      .withColumn("volume_last_week", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(13)), $"session_volume").otherwise(lit(null).cast(LongType)))
      .withColumn("is_week", when(($"session_day_diff" >= lit(7)) && ($"session_day_diff" <= lit(13)), lit(1)).otherwise(lit(0).cast(LongType)))
      .groupBy("hotspot_ident_code")
      .agg(
        sum("sessions_last_3_months").alias("sessions_last_3_months"),
        sum("volume_last_3_months").alias("volume_last_3_months"),
        sum("sessions_last_month").alias("sessions_last_month"),
        sum("volume_last_month").alias("volume_last_month"),
        sum("sessions_last_week").alias("sessions_last_week"),
        sum("volume_last_week").alias("volume_last_week"),
        max("is_3_months").alias("is_3_months"),
        max("is_month").alias("is_month"),
        max("is_week").alias("is_week")
      )
      .filter($"is_3_months" > 0 || $"is_month" > 0 || $"is_week" > 0)
      .drop("is_3_months", "is_month", "is_week")
    // .na.fill(0,Seq("volume_last_week","volume_last_month","volume_last_3_months","sessions_last_week","sessions_last_month","sessions_last_3_months"))
  }

  private val data = {
    sessionDData
      .withColumn("weekday", dayofweek($"wlan_session_date") - lit(1))
      .withColumn("session_day_diff", getSessionDayDiff($"weekday"))
      .withColumnRenamed("wlan_hotspot_ident_code", "hotspot_ident_code")
      .select("wlan_provider_code", winaColumns: _*)

  }

  val getTMDData = aggregateWina(data.filter($"wlan_provider_code".equalTo("TMD") || ($"wlan_provider_code".equalTo("TWLAN_DE"))))
  val getTCOMData = aggregateWina(data.filter($"wlan_provider_code".equalTo("TCOM")))

}
