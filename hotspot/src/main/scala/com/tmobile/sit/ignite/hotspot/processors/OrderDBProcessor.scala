package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.Reader
import com.tmobile.sit.ignite.hotspot.data.{OrderDBStage, OrderDBStructures, WlanHotspotTypes}
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{lit, unix_timestamp, max}

class OrderDBProcessor(inputHotspot: Reader, inputMPS: Reader, inputErrorCodes: Reader, maxDate: Timestamp)(implicit sparkSession: SparkSession) extends Logger {
  def mapWlanHotspotStage(data: Dataset[OrderDBStructures.OrderDBInput]): Dataset[OrderDBStage] = {
    import sparkSession.implicits._
    data.filter(i => !(i.result_code.get == "KO" && !i.error_code.isDefined) && !(i.reduced_amount.isDefined && !i.campaign_name.isDefined))
      .map(i => OrderDBStage(i))
  }

  def processData(): Unit = {
    import sparkSession.implicits._
    val data = new OrderDBData(orderDbReader = inputMPS, oldErrorCodes = inputErrorCodes, inputHotspot = inputHotspot)

    val wlanHotspotNew= mapWlanHotspotStage(data.fullData)
      .select($"hotspot_ident_code", $"hotspot_timezone", $"hotspot_venue_type_code", $"hotspot_venue_code", $"hotspot_provider_code", $"hotspot_country_code", $"hotspot_city_code", $"ta_request_date")
      .withColumn("valid_from", unix_timestamp($"ta_request_date").cast(TimestampType))
      .withColumn("valid_to", lit(maxDate).cast(TimestampType))
      .withColumn("wlan_hotspot_id", lit(0))
      .drop("ta_request_date")
      .distinct()
      .filter(!$"hotspot_ident_code".startsWith(lit("undefined_OTHER_OTHER")))
      .as[WlanHotspotTypes.WlanHotspotStage]
      .map(r => WlanHotspotTypes.WlanHostpot(r))

    //map_1, unique pres identCode, map_2

    val wlanHotspotOld = inputHotspot.read().as[WlanHotspotTypes.WlanHostpot]

    val ret = wlanHotspotOld.union(wlanHotspotNew)

    ret.select(max($"valid_to")).show(false)

  }
}
