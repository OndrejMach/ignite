package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.ignite.hotspot.data.{FUTURE, OutputStructures}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{concat, count, first, lit, monotonically_increasing_id, sum, upper, when}
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import java.sql.{Date, Timestamp}

import com.tmobile.sit.common.Logger

case class SessionDOutputs(sessionD:DataFrame, wlanHotspotData: DataFrame)

class SessionDProcessor(cdrData: DataFrame, orderdDBData: OderdDBPRocessingOutputs,processingDate: Date )(implicit sparkSession: SparkSession) extends Logger{
  import sparkSession.implicits._

  private val cdrAggregates = {
  logger.info("Geting CDR data Aggregates")
    cdrData
      .withColumn("wlan_hotspot_ident_code", when($"hotspot_id".isNotNull, $"hotspot_id").otherwise(concat(lit("undefined_"), $"hotspot_owner_id")))
      .withColumn("stop_ticket", when($"terminate_cause_id".equalTo(lit(1001)), 1).otherwise(0))
      .select($"wlan_session_date",
        $"wlan_hotspot_ident_code",
        $"hotspot_owner_id".as("wlan_provider_code"),
        $"wlan_user_account_id",
        $"user_provider_id".as("wlan_user_provider_code"),
        $"terminate_cause_id",
        $"login_type",
        $"session_duration",
        $"session_volume",
        $"venue_type",
        $"venue",
        $"country_code",
        $"stop_ticket",
        $"english_city_name")
      .sort()
      .groupBy("wlan_session_date", "wlan_user_provider_code", "wlan_provider_code", "wlan_hotspot_ident_code", "wlan_user_account_id", "terminate_cause_id", "login_type")
      .agg(
        first("venue_type").alias("venue_type"),
        first("venue").alias("venue"),
        sum("session_duration").alias("session_duration"),
        sum("session_volume").alias("session_volume"),
        first("country_code").alias("country_code"),
        first("english_city_name").alias("english_city_name"),
        sum("stop_ticket").alias("num_of_gen_stop_tickets"),
        count("*").alias("num_of_stop_tickets")
      )
      .withColumn("num_subscriber", lit(1).cast(IntegerType))

  }

  private val wlanHotspotData = {
    logger.info("Filtering wlan hotspot data for today")
    val todayDataHotspot = orderdDBData.wlanHotspot.filter($"valid_to" >= lit(processingDate).cast(TimestampType))
    logger.info("Filtering wlan hotspot data for history")
    val oldDataHotspot = orderdDBData.wlanHotspot.filter(!($"valid_to" >= lit(processingDate).cast(TimestampType)))

    val aggColumns = cdrAggregates.columns.map("agg_" + _)

    logger.info("Joining with CDR Aggregates")
    val sessionDOut = todayDataHotspot
      .join(cdrAggregates.toDF(aggColumns: _*), $"wlan_hotspot_ident_code" === $"agg_wlan_hotspot_ident_code", "outer")
      .withColumn("wlan_venue_type_code", when(($"wlan_venue_type_code" =!= $"agg_venue_type") && $"agg_venue_type".isNotNull, $"agg_venue_type").otherwise($"wlan_venue_type_code"))
      .withColumn("wlan_venue_code", when(($"wlan_venue_code" =!= $"agg_venue") && $"agg_venue".isNotNull, $"agg_venue").otherwise($"wlan_venue_code"))
      .withColumn("city_code", when(($"city_code" =!= $"agg_english_city_name") && $"agg_english_city_name".isNotNull, $"agg_english_city_name").otherwise($"city_code"))
      .withColumn("valid_to", when($"agg_wlan_session_date".isNotNull, $"agg_wlan_session_date").otherwise(lit(FUTURE)))

    val both = sessionDOut.filter($"wlan_hotspot_id".isNotNull)
    val toProvision = sessionDOut.filter($"wlan_hotspot_id".isNull)

    logger.info("Preparing new wlanHostpot data from CDR aggregates")
    toProvision
      .withColumn("wlan_hotspot_id", monotonically_increasing_id())
      .withColumn("wlan_hotspot_id", $"wlan_hotspot_id" + lit(orderdDBData.maxHotspotID))
      .withColumn("wlan_hotspot_desc" ,lit( "Hotspot not assigned"))
      .withColumn("country_code", upper($"country_code"))
      .withColumn("wlan_provider_code", $"agg_wlan_provider_code")
      .withColumn("valid_from", $"agg_wlan_session_date".cast(TimestampType))
      .na.fill("undefined", Seq("wlan_venue_type_code", "wlan_venue_code","city_code"))

    logger.info("Getting the new WLanHotspot Data")
    both
      .union(toProvision)
      .select(todayDataHotspot.columns.head, todayDataHotspot.columns.tail :_*)
      .union(oldDataHotspot)
  }


  def processData(): SessionDOutputs = {
    SessionDOutputs(sessionD = cdrAggregates.select(OutputStructures.SESSION_D_OUTPUT_COLUMNS.head, OutputStructures.SESSION_D_OUTPUT_COLUMNS.tail :_*), wlanHotspotData = wlanHotspotData)
  }

}
