package com.tmobile.sit.ignite.hotspot.processors

import java.sql.{Date, Timestamp}

import com.tmobile.sit.common.readers.CSVReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

class FailedTransactionsProcessor(orderDBData: DataFrame, orderDBPLus1: DataFrame, wlanHostspot: DataFrame, processingDate: Date)(implicit sparkSession: SparkSession) {

  val CITY_STRUCT = StructType {
    Seq(
      StructField("city_id", LongType, true),
      StructField("city_code", StringType, true),
      StructField("city_desc", StringType, true),
      StructField("city_ldesc", StringType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  }

  val VOUCHER_STRUCT = StructType {
    Seq(
      StructField("wlan_voucher_id", LongType, true),
      StructField("wlan_voucher_code", StringType, true),
      StructField("wlan_voucher_desc", StringType, true),
      StructField("tmo_country_code", StringType, true),
      StructField("duration", StringType, true),
      StructField("price", DoubleType, true),
      StructField("vat", DoubleType, true),
      StructField("conversion", DoubleType, true),
      StructField("valid_from", TimestampType, true),
      StructField("valid_to", TimestampType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  }

  def processData(): Unit = {
    import sparkSession.implicits._
    val cityData = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/common/cptm_ta_d_city.csv", header = false, schema = Some(CITY_STRUCT), delimiter = "|")
      .read()
      .sort("city_id")

    val maxCityId = cityData.select(max("city_id")).first().getLong(0)

    val voucherData = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/stage/cptm_ta_d_wlan_voucher.csv", header = false, schema = Some(VOUCHER_STRUCT), delimiter = "|")
      .read()
      .withColumnRenamed("tmo_country_code", "natco")
      .withColumnRenamed("price", "amount")
      .withColumnRenamed("wlan_voucher_desc", "voucher_type")
      .sort("natco", "voucher_type", "amount", "duration")

    val maxVoucherID = voucherData.select(max("wlan_voucher_id")).first().getLong(0)

    voucherData.show(false)

    val hotspotData = wlanHostspot
      .select("wlan_hotspot_id", "wlan_hotspot_ident_code", "wlan_venue_type_code", "wlan_venue_code", "wlan_provider_code", "country_code", "city_code", "valid_from", "valid_to")
      .sort("wlan_hotspot_ident_code", "valid_from", "valid_to")
      .groupBy("wlan_hotspot_ident_code")
      .agg(
        first("wlan_venue_type_code").alias("wlan_venue_type_code"),
        first("wlan_venue_code").alias("wlan_venue_code"),
        first("wlan_provider_code").alias("wlan_provider_code"),
        first("country_code").alias("country_code"),
        first("city_code").alias("city_code"),
        first("valid_from").alias("valid_from"),
        first("valid_to").alias("valid_to")
      )

    val allOrderDB = orderDBData
      .union(orderDBPLus1)
      .filter($"ta_request_date" === lit(processingDate))
      .filter($"result_code".isNotNull)
      .na.fill("Undefined", Seq("currency", "payment_method", "voucher_type", "card_institute"))
      .withColumn("vat", when($"vat".isNull, lit(-9999999)).otherwise($"vat"))
      .withColumn("voucher_duration", when($"voucher_duration" >= lit(900), $"voucher_duration" / lit(3600)).otherwise($"voucher_duration"))
      .withColumn("wlan_transac_type_id", when($"cancellation".isNotNull && $"cancellation".equalTo(lit("X")), lit(1)).otherwise(lit(1)))
      .withColumnRenamed("alternate_amount", "number_miles")

    val wlanHotspotOrderDB = hotspotData.join(allOrderDB, $"wlan_hotspot_ident_code" === $"hotspot_ident_code", "inner")

    val newCities = wlanHotspotOrderDB
      .select("city_code")
        .distinct()
        .join(cityData, Seq("city_code"), "left_outer")
        .filter($"city_id".isNull)
        .withColumn("city_id", monotonically_increasing_id() + lit(maxCityId))
        .withColumn("city_desc", upper($"city_code"))
        .withColumn("city_ldesc", lit("new"))
        .withColumn("load_date", lit(processingDate).cast(TimestampType))
        .withColumn("entry_id", lit(1))

    val cities = newCities.union(cityData)



    wlanHotspotOrderDB.printSchema()
    wlanHotspotOrderDB.show(false)

    println(s"${maxCityId} ${maxVoucherID}")
  }
}
