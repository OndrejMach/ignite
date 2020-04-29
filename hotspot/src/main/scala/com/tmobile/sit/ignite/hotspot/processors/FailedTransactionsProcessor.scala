package com.tmobile.sit.ignite.hotspot.processors

import java.sql.{Date, Timestamp}

import com.tmobile.sit.ignite.common.processing.translateHours
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.common.processing.NormalisedExchangeRates
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

class FailedTransactionsProcessor(orderDBData: DataFrame, orderDBPLus1: DataFrame, wlanHostspot: DataFrame, normalisedExchangeRates: NormalisedExchangeRates, processingDate: Date)(implicit sparkSession: SparkSession) {

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
      // .withColumnRenamed("duration", "voucher_duration")
      .drop("entry_id", "load_date")
      .sort("natco", "voucher_type", "amount", "duration")

    //voucherData.show(false)

    val maxVoucherID = voucherData.select(max("wlan_voucher_id")).first().getLong(0)

    //voucherData.show(false)

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
        first("valid_to").alias("valid_to"),
        first("wlan_hotspot_id").alias("wlan_hotspot_id")

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
      .withColumnRenamed("card_institute", "wlan_card_institute_code")
      .withColumnRenamed("payment_method", "wlan_payment_type_code")
      .withColumnRenamed("error_code", "wlan_error_code")

    allOrderDB.printSchema()


    import org.apache.spark.sql.functions.udf
    val hoursToString = udf { l: Long => translateHours(l) }

    val wlanHotspotOrderDB =
      hotspotData
        .join(allOrderDB, $"wlan_hotspot_ident_code" === $"hotspot_ident_code", "inner")
        .na.fill("UNDEFINED", Seq("wlan_venue_type_code", "wlan_venue_code", "wlan_provider_code"))
        .withColumn("wlan_venue_type_code", when(upper($"wlan_venue_type_code").equalTo(lit("UNDEFINED")), lit("UNDEFINED")).otherwise($"wlan_venue_type_code"))
        .withColumn("wlan_venue_code", when(upper($"wlan_venue_code").equalTo(lit("UNDEFINED")), lit("UNDEFINED")).otherwise($"wlan_venue_code"))
        .withColumn("wlan_provider_code", when(upper($"wlan_provider_code").equalTo(lit("UNDEFINED")), lit("UNDEFINED")).otherwise($"wlan_provider_code"))
        .withColumn("duration", hoursToString($"voucher_duration"))
        .withColumn("wlan_voucher_code", concat_ws("_", $"voucher_type", $"natco", $"voucher_duration"))


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


    //toReturn
    val cities = newCities.union(cityData)

    //wlanHotspotOrderDB.printSchema()

    val wlanHotspotOrderDBWithExchangeRates =
      normalisedExchangeRates.joinWithExchangeRates(wlanHotspotOrderDB.withColumnRenamed("valid_from", "wlan_valid_from").withColumnRenamed("valid_to", "wlan_valid_to"))
        .drop("valid_from", "valid_to")
        .withColumnRenamed("wlan_valid_from", "valid_from")
        .withColumnRenamed("wlan_valid_to", "valid_to")

    val KEY_COLUMNS_VOUCHER = Seq("natco", "voucher_type", "amount", "voucher_duration")
    val COLUMNS_VOUCHER = Seq("wlan_voucher_code", "voucher_type", "natco", "duration", "amount", "vat", "conversion", "valid_from", "valid_to")
    val JOIN_COLUMNS_VOUCHER = Seq("natco", "voucher_type", "amount", "duration")

    val newVouchers = wlanHotspotOrderDBWithExchangeRates
      //.withColumnRenamed("duration", "voucher_duration")
      .sort(KEY_COLUMNS_VOUCHER.head, KEY_COLUMNS_VOUCHER.tail: _*)
      .dropDuplicates(KEY_COLUMNS_VOUCHER.head, KEY_COLUMNS_VOUCHER.tail: _*)
      .select(COLUMNS_VOUCHER.head, COLUMNS_VOUCHER.tail: _*)
      .join(voucherData.select("wlan_voucher_id", JOIN_COLUMNS_VOUCHER: _*), Seq("natco", "voucher_type", "amount", "duration"), "left_outer")
      .filter($"wlan_voucher_id".isNull)
      .withColumn("wlan_voucher_id", monotonically_increasing_id() + lit(maxVoucherID))
      .select("wlan_voucher_id", COLUMNS_VOUCHER: _*)

    val retVoucher = newVouchers.union(voucherData.select("wlan_voucher_id", COLUMNS_VOUCHER: _*))


    val transactionsData =
      wlanHotspotOrderDBWithExchangeRates
        .join(
          retVoucher.select("wlan_voucher_id", JOIN_COLUMNS_VOUCHER: _*),
          JOIN_COLUMNS_VOUCHER,
          "left_outer")
        .withColumn("reduced_amount", when($"reduced_amount".isNotNull, $"reduced_amount").otherwise($"amount"))
        .withColumn("discount_rel", concat(((($"amount" - $"reduced_amount") * 100) / round($"amount", 2)).cast(StringType), lit("%")))
        .na.fill("No Discount", Seq("campaign_name"))

    //transactionsData.show(false)

    val OKTransactions = transactionsData.filter($"result_code".equalTo("OK"))
    val failedTransactions = transactionsData.filter(!$"result_code".equalTo("OK"))

    val KEY_AGG_ORDERDB_H = Seq("request_hour",
      "wlan_hotspot_id",
      "country_code",
      "city_code",
      "wlan_provider_code",
      "wlan_venue_type_code",
      "wlan_venue_code",
      "wlan_voucher_id",
      "wlan_card_institute_code",
      "currency",
      "vat",
      "wlan_payment_type_code",
      "voucher_type",
      "wlan_transac_type_id",
      "campaign_name",
      "discount_rel")

    val orderDBH = OKTransactions
      .withColumn("request_hour", $"ta_request_datetime")
      .groupBy(KEY_AGG_ORDERDB_H.head, KEY_AGG_ORDERDB_H.tail: _*)
      .agg(
        sum("number_miles").alias("num_flight_miles"),
        count("*").alias("num_of_transactions"),
        //tech columns
        sum("amount").alias("sum_amount"),
        sum("reduced_amount").alias("sum_red_amount"),
        sum($"amount" - $"reduced_amount").alias("sum_amount_red_amount"),
        first("conversion").alias("conversion")
      )
      .join(cities.select("city_id", "city_code"), Seq("city_code"), "left_outer")
      .withColumn("t_vat", $"vat" / lit(100) + lit(1))
      .withColumn("wlan_voucher_type", $"voucher_type")
      .withColumn("amount_d_incl_vat", round($"sum_amount_red_amount" * $"conversion", 2))
      .withColumn("amount_d_excl_vat", round($"sum_amount_red_amount" * $"conversion" / $"t_vat", 2))
      .withColumn("amount_d_incl_vat_lc", round($"sum_amount_red_amount", 2))
      .withColumn("amount_d_excl_vat_lc", round($"sum_amount_red_amount" / $"t_vat", 2))
      .withColumn("amount_c_incl_vat", round($"sum_red_amount" * $"conversion", 2))
      .withColumn("amount_c_excl_vat", round($"sum_red_amount" * $"conversion" / $"t_vat", 2))
      .withColumn("amount_c_incl_vat_lc", round($"sum_red_amount", 2))
      .withColumn("amount_c_excl_vat_lc", round($"sum_red_amount" / $"t_vat", 2))
      .withColumn("amount_incl_vat", round($"sum_amount" * $"conversion", 2))
      .withColumn("amount_excl_vat", round($"sum_amount" * $"conversion" / $"t_vat", 2))
      .withColumn("amount_incl_vat_lc", round($"sum_amount", 2))
      .withColumn("amount_excl_vat_lc", round($"sum_amount" / $"t_vat", 2))

    //orderDBH.show(false)


    val KEY_AGG_FAILED_TRANSAC = Seq("request_hour",
      "wlan_hotspot_id",
      "country_code",
      "city_code",
      "wlan_provider_code",
      "wlan_venue_type_code",
      "wlan_venue_code",
      "wlan_voucher_id",
      "wlan_card_institute_code",
      "currency",
      "vat",
      "wlan_payment_type_code",
      "voucher_type",
      "wlan_transac_type_id",
      "wlan_error_code")


    val failedTrans = failedTransactions
      .withColumn("request_hour", $"ta_request_datetime")
      .groupBy(KEY_AGG_FAILED_TRANSAC.head, KEY_AGG_FAILED_TRANSAC.tail: _*)
      .agg(
        count("*").alias("num_of_failed_transac"),
        sum("number_miles").alias("num_flight_miles")
      )
      .join(cities.select("city_id", "city_code"), Seq("city_code"), "left_outer")
      .withColumn("wlan_voucher_type", $"voucher_type")

    failedTrans.show(false)

    // wlanHotspotOrderDB.printSchema()
    //wlanHotspotOrderDB.show(false)

    //println(s"${maxCityId} ${maxVoucherID}")

    //return
    // (cities,retVoucher,orderDBH,failedTrans)
  }
}
