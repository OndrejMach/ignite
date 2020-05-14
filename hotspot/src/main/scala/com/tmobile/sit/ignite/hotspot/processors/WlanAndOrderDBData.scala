package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Date

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{first, lit, max, when, min, last}
import org.apache.spark.sql.types.DateType

class WlanAndOrderDBData(wlanHotspotData: DataFrame, orderDbDataActual: DataFrame)(implicit sparkSession: SparkSession, processingDate: Date) extends Logger {

  import sparkSession.implicits._

  val hotspotData = {
    logger.info("Preparing WLAN hotspot data for processing")
    wlanHotspotData
      .select("wlan_hotspot_id", "wlan_hotspot_ident_code", "wlan_venue_type_code", "wlan_venue_code", "wlan_provider_code", "country_code", "city_code", "valid_from", "valid_to")
      .sort("wlan_hotspot_ident_code", "valid_from", "valid_to")
      .groupBy("wlan_hotspot_ident_code")
      .agg(
        last("wlan_venue_type_code").alias("wlan_venue_type_code"),
        last("wlan_venue_code").alias("wlan_venue_code"),
        last("wlan_provider_code").alias("wlan_provider_code"),
        last("country_code").alias("country_code"),
        last("city_code").alias("city_code"),
        last("valid_from").alias("valid_from"),
        last("valid_to").alias("valid_to"),
        last("wlan_hotspot_id").alias("wlan_hotspot_id")

      )
  }

  val allOrderDB = {
    logger.info("Preparing orderDB data for processing")
    orderDbDataActual
     // .union(orderDBDataDayPLus1)
      .filter($"ta_request_date" === lit(processingDate).cast(DateType))
      .filter($"result_code".isNotNull && !$"result_code".equalTo(lit("")))
      .na.fill("Undefined", Seq("currency", "payment_method", "voucher_type", "card_institute"))
      .withColumn("vat", when($"vat".isNull, lit(-9999999)).otherwise($"vat"))
      .withColumn("voucher_duration", when($"voucher_duration" >= lit(900), $"voucher_duration" / lit(3600)).otherwise($"voucher_duration"))
      .withColumn("wlan_transac_type_id", when($"cancellation".isNotNull && $"cancellation".equalTo(lit("X")), lit(1)).otherwise(lit(0)))
      .withColumnRenamed("alternate_amount", "number_miles")
      .withColumnRenamed("card_institute", "wlan_card_institute_code")
      .withColumnRenamed("payment_method", "wlan_payment_type_code")
      .withColumnRenamed("error_code", "wlan_error_code")
  }
}