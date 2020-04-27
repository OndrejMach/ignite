package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.data.{OrderDBInputData, OrderDBStage, OrderDBStructures, WlanHotspotTypes}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}


case class OderdDBPRocessingOutputs(wlanHotspot: DataFrame, errorCodes: DataFrame, mapVoucher: DataFrame, orderDb: DataFrame, maxHotspotID: Long)

class OrderDBProcessor( orderDBInputData: OrderDBInputData,maxDate: Timestamp)(implicit sparkSession: SparkSession) extends Logger {
  import sparkSession.implicits._
  private def mapWlanHotspotStage(data: Dataset[OrderDBStructures.OrderDBInput]): Dataset[OrderDBStage] = {
    logger.info("Filtering and preparing input data from input file")
    data.filter(i => !(i.result_code.get == "KO" && !i.error_code.isDefined) && !(i.reduced_amount.isDefined && !i.campaign_name.isDefined))
      .map(i => OrderDBStage(i))
  }

  private def preprocessWlanHotspotStage(stageWlanHotspot: Dataset[OrderDBStage]) : Dataset[WlanHotspotTypes.WlanHotspotStage] = {
    logger.info("Preparing input data for processing.. staging..")
    stageWlanHotspot
    .select($"hotspot_ident_code",
      $"hotspot_timezone",
      $"hotspot_venue_type_code",
      $"hotspot_venue_code",
      $"hotspot_provider_code",
      $"hotspot_country_code",
      $"hotspot_city_code",
      $"ta_request_date")
      .withColumn("valid_from_n", unix_timestamp($"ta_request_date").cast(TimestampType))
      .withColumn("valid_to_n", lit(maxDate).cast(TimestampType))
      .withColumn("hotspot_id", lit(0))
      .drop("ta_request_date")
      .distinct()
      .filter(!$"hotspot_ident_code".startsWith(lit("undefined_OTHER_OTHER")))
      .as[WlanHotspotTypes.WlanHotspotStage]
  }

  private def joinWlanHotspotData(wlanHotspotNew: Dataset[WlanHotspotTypes.WlanHotspotStage], wlanHotspotOld: Dataset[WlanHotspotTypes.WlanHostpot], maxId: Long) : DataFrame = {
    def getField(nameNew: String, nameOld: String): Column = {
      when(wlanHotspotNew(nameNew).isNotNull,wlanHotspotNew(nameNew)).otherwise(wlanHotspotOld(nameOld))
    }
   // val maxId = data.hotspotIDsSorted.first().getLong(0)
    logger.info("Old Wlan data to be joined with new data from order_db")
    val merge = wlanHotspotOld
      .join(wlanHotspotNew.withColumnRenamed("hotspot_ident_code","wlan_hotspot_ident_code"),
        Seq("wlan_hotspot_ident_code"), "full_outer")
      .withColumn("valid_from", getField("valid_from_n","valid_from" ))
      .withColumn("valid_to", getField("valid_to_n","valid_to"))
      .withColumn("hotspot_timezone", getField("hotspot_timezone", "wlan_hotspot_timezone"))
      .withColumn("hotspot_venue_type_code", getField("hotspot_venue_type_code","wlan_venue_type_code" ))
      .withColumn("hotspot_venue_code", getField("hotspot_venue_code", "wlan_venue_code"))
      .withColumn("hotspot_provider_code", getField("hotspot_provider_code", "wlan_provider_code"))
      .withColumn("hotspot_country_code", getField("hotspot_country_code", "country_code"))
      .withColumn("hotspot_city_code", getField("hotspot_city_code", "city_code"))
      .drop("hotspot_timezone","hotspot_venue_type_code","hotspot_venue_code", "hotspot_provider_code", "hotspot_country_code", "hotspot_city_code", "hotspot_id","valid_to_n", "valid_from_n" )

   logger.info(s"Calculating new hotspotIDs based on maxId from previous runs ${maxId}")
    val forNewIDs = merge
      .filter("wlan_hotspot_id is null")
      .select("wlan_hotspot_ident_code")
      .sort()
      .withColumn("id", monotonically_increasing_id().cast(LongType))
      .withColumn("id", $"id" + lit(maxId))

    logger.info("Joining data with new IDs wiht old wlan hotspot data")
    merge
      .join(forNewIDs, Seq("wlan_hotspot_ident_code"), "left_outer")
      .withColumn("wlan_hotspot_id", when($"wlan_hotspot_id".isNull, $"id").otherwise($"wlan_hotspot_id"))
      .drop("id")
  }

  private def mapVoucher(data: Dataset[OrderDBStructures.OrderDBInput]) = {
    data
      .filter(!($"result_code".notEqual(lit("OK")) || $"cancellation".isNotNull))
      .withColumn("tmp_username", split($"username", "@"))
      .withColumn("wlif_username", when($"username".contains("@"),sha2($"tmp_username".getItem(0),224)))
      .withColumn("wlif_realm_code", when($"username".contains("@"),$"tmp_username".getItem(1)))
      .withColumn("wlan_username", trim($"username"))
      .na.fill("#",Seq("wlan_username"))
      .withColumn("wlan_ta_id", $"transaction_id")
      .withColumn("wlan_request_date", to_timestamp(concat($"transaction_date", $"transaction_time"), "yyyyMMddHHmmss"))
  }

  def processData(): OderdDBPRocessingOutputs = {
    logger.info("Preparing input data - orderDB, wlanhotspot data, error codes and old error codes list file")
    val data = new OrderDBData(orderDbReader = orderDBInputData.inputMPSReader, oldErrorCodes = orderDBInputData.oldErrorCodesReader, inputHotspot = orderDBInputData.dataHotspotReader)
    logger.info("Reading and filtering input data")
    val wlanHotspotNew= mapWlanHotspotStage(data.fullData)
    logger.info("Preprocessing input data")
    val preprocessedWlanHotspot = preprocessWlanHotspotStage(wlanHotspotNew)
    logger.info("Reading old wlan hotspot data")
    val wlanHotspotOld = orderDBInputData.dataHotspotReader.read().as[WlanHotspotTypes.WlanHostpot]
    logger.info("Joining new wlan hotspot data with the old wlan hotspot set")
    val maxId = data.hotspotIDsSorted.first().getLong(0)
    val wlanData = joinWlanHotspotData(preprocessedWlanHotspot,wlanHotspotOld, maxId)
    logger.info("Generating outputs - wlan hotspot file and error code list file")
    OderdDBPRocessingOutputs(
      wlanHotspot = wlanData, //cptm_ta_d_wlan_hotspot
      errorCodes= data.allOldErrorCodes.union(data.newErrorCodes), //cptm_ta_d_wlan_error_code
      mapVoucher = mapVoucher(data.fullData), //cptm_ta_f_wlif_map_voucher
      orderDb = wlanHotspotNew.toDF(),
      maxHotspotID = maxId
    ) //cptm_ta_f_wlan_orderdb
  }
}
