package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.data.{FUTURE, StageStructures}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class for Session D data calculation
 * @param cdrData - data from the CDR input
 * @param wlanHotspotStageData - wlan hotspot data
 * @param processingDate - data date for calculation
 * @param sparkSession
 */

class SessionDProcessor(cdrData: DataFrame, wlanHotspotStageData: DataFrame, processingDate: Date)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  private val cdrAggregates = {
    logger.info(s"Geting CDR data Aggregates - input size: ${cdrData.count()}")
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
    val maxHotspotID = wlanHotspotStageData.select(max($"wlan_hotspot_id")).first().getLong(0)

    logger.info("Filtering wlan hotspot data for today")
    val todayDataHotspot = wlanHotspotStageData.filter($"valid_to" >= lit(processingDate).cast(TimestampType))
    logger.info("Filtering wlan hotspot data for history")
    val oldDataHotspot = wlanHotspotStageData.filter(($"valid_to".isNull) || ($"valid_to" < lit(processingDate).cast(TimestampType)))

    logger.info(s"Today HOTSPOT data count: ${todayDataHotspot.count()}")
    logger.info(s"Old HOTSPOT data count: ${oldDataHotspot.count()}")

    //cdrAggregates.printSchema()

    // val aggColumns = cdrAggregates.columns.map("agg_" + _)

    val uniqueCDRs: DataFrame = cdrAggregates
      .sort("wlan_hotspot_ident_code")
      .groupBy("wlan_hotspot_ident_code")
      .agg(
        first("wlan_session_date").alias("wlan_session_date"),
        first("wlan_user_provider_code").alias("wlan_user_provider_code"),
        first("wlan_provider_code").alias("wlan_provider_code"),
        first("wlan_user_account_id").alias("wlan_user_account_id"),
        first("terminate_cause_id").alias("terminate_cause_id"),
        first("login_type").alias("login_type"),
        first("venue_type").alias("venue_type"),
        first("venue").alias("venue"),
        first("session_duration").alias("session_duration"),
        first("session_volume").alias("session_volume"),
        first("country_code").alias("country_code"),
        first("english_city_name").alias("english_city_name"),
        first("num_of_gen_stop_tickets").alias("num_of_gen_stop_tickets"),
        first("num_of_stop_tickets").alias("num_of_stop_tickets"),
        first("num_subscriber").alias("num_subscriber")
      )

      val toGo = uniqueCDRs.toDF(uniqueCDRs.columns.map("agg_" + _) :_*)

    // val aggColumns =  uniqueCDRs.columns.map("agg_" + _)

    logger.info("Joining with CDR Aggregates")
    val sessionDOut = todayDataHotspot
      .join(toGo, $"wlan_hotspot_ident_code" === $"agg_wlan_hotspot_ident_code", "left_outer")
      .withColumn("wlan_venue_type_code", when(($"wlan_venue_type_code" =!= $"agg_venue_type") && $"agg_venue_type".isNotNull, $"agg_venue_type").otherwise($"wlan_venue_type_code"))
      .withColumn("wlan_venue_code", when(($"wlan_venue_code" =!= $"agg_venue") && $"agg_venue".isNotNull, $"agg_venue").otherwise($"wlan_venue_code"))
      .withColumn("city_code", when(($"city_code" =!= $"agg_english_city_name") && $"agg_english_city_name".isNotNull, $"agg_english_city_name").otherwise($"city_code"))
      .withColumn("valid_to", when($"agg_wlan_session_date".isNotNull, $"agg_wlan_session_date").otherwise(lit(FUTURE)))
      .select(todayDataHotspot.columns.head, todayDataHotspot.columns.tail: _*)

    val onlyCDRs = toGo.join(todayDataHotspot, $"wlan_hotspot_ident_code" === $"agg_wlan_hotspot_ident_code", "left_outer")

    //val both = onlyCDRs.filter($"wlan_hotspot_id".isNotNull)

    val toProvision = onlyCDRs.filter($"wlan_hotspot_id".isNull)

    //toProvision.show(false)


    logger.info(s"With HOTSPOT_ID data count: ${sessionDOut.count()}")
    logger.info(s"With HOTSPOT_ID data count: ${toProvision.count()}")

    logger.info("Preparing new wlanHostpot data from CDR aggregates")
    val provisioned = toProvision
      .withColumn("wlan_hotspot_id", monotonically_increasing_id())
      .withColumn("wlan_hotspot_id", $"wlan_hotspot_id" + lit(maxHotspotID))
      .withColumn("wlan_hotspot_desc", lit("Hotspot not assigned"))
      .withColumn("country_code", upper($"country_code"))
      .withColumn("wlan_provider_code", $"agg_wlan_provider_code")
      .withColumn("valid_from", $"agg_wlan_session_date".cast(TimestampType))
      .withColumn("wlan_hotspot_ident_code", when($"wlan_hotspot_ident_code".isNull,$"agg_wlan_hotspot_ident_code" ).otherwise($"wlan_hotspot_ident_code"))
      .na.fill("undefined", Seq("wlan_venue_type_code", "wlan_venue_code", "city_code"))
      .select(todayDataHotspot.columns.head, todayDataHotspot.columns.tail: _*)

    logger.info("Getting the new WLanHotspot Data")

    logger.info("PROVISIONED NULL HOTSPOTS: "+provisioned.filter("wlan_hotspot_ident_code is null").count())
    logger.info("sessionDOut NULL HOTSPOTS: "+sessionDOut.filter("wlan_hotspot_ident_code is null").count())
    logger.info("oldDataHotspot NULL HOTSPOTS: "+oldDataHotspot.filter("wlan_hotspot_ident_code is null").count())

    val ret = provisioned
      .union(sessionDOut)
      .union(oldDataHotspot)

    logger.info(s"New HOTSPOT file row count: ${ret.count()}")
    ret
  }


  def processData(): (DataFrame, DataFrame) = {
    (cdrAggregates.select(StageStructures.SESSION_D_OUTPUT_COLUMNS.head, StageStructures.SESSION_D_OUTPUT_COLUMNS.tail: _*), wlanHotspotData)
  }

}
