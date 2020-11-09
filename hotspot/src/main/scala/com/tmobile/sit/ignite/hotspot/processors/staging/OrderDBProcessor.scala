package com.tmobile.sit.ignite.hotspot.processors.staging

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.hotspot.data.{OrderDBInputData, OrderDBStage, OrderDBStructures, WlanHotspotTypes}
import com.tmobile.sit.ignite.hotspot.processors.udfs.DirtyStuff
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}


case class OderdDBPRocessingOutputs(wlanHotspot: DataFrame, errorCodes: DataFrame, mapVoucher: DataFrame, orderDb: DataFrame, maxHotspotID: Long)

class OrderDBProcessor(orderDBInputData: OrderDBInputData, maxDate: Timestamp, encoderPath: String)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  val encoder3des = udf(DirtyStuff.encode)


  private def mapWlanHotspotStage(data: Dataset[OrderDBStructures.OrderDBInput]): Dataset[OrderDBStage] = {
    logger.info(s"Filtering and preparing input data from input file ${data.count()}")
    val ret =
      data
        .filter(i => !(i.result_code.get == "KO" && !i.error_code.isDefined) )
        .filter(i => !(i.reduced_amount.isDefined && !i.campaign_name.isDefined))
        .map(i => OrderDBStage(i))
        .withColumn("email", when($"email".equalTo(lit("#")), lit("#")).otherwise(regexp_replace(encoder3des(lit(encoderPath), $"email"), "[\n\r]", "")))
        .withColumn("username", when($"username".equalTo(lit("#")), lit("#")).otherwise(regexp_replace(encoder3des(lit(encoderPath), $"username"), "[\n\r]", "")))
        .withColumn("ma_name", when($"ma_name".equalTo(lit("#")), lit("#")).otherwise(regexp_replace(encoder3des(lit(encoderPath), $"ma_name"), "[\n\r]", "")))
        .as[OrderDBStage]

    ret.select("hotspot_country_code").distinct().collect().foreach(println(_))

    logger.info(s"Filtered data size ${ret.count()}")
    ret
  }

  private def preprocessWlanHotspotStage(stageWlanHotspot: Dataset[OrderDBStage]): Dataset[WlanHotspotTypes.WlanHotspotStage] = {
    logger.info("Preparing input data for processing.. staging..")
    val ret = stageWlanHotspot
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
      .drop("ta_request_date")
      .filter($"hotspot_ident_code".startsWith(lit("undefined_OTHER_OTHER")))
      .sort()
      .groupBy("hotspot_ident_code")
      .agg(
        first("hotspot_timezone").alias("hotspot_timezone"),
        first("hotspot_venue_type_code").alias("hotspot_venue_type_code"),
        first("hotspot_venue_code").alias("hotspot_venue_code"),
        first("hotspot_provider_code").alias("hotspot_provider_code"),
        first("hotspot_country_code").alias("hotspot_country_code"),
        first("hotspot_city_code").alias("hotspot_city_code"),
        first("valid_from_n").alias("valid_from_n"),
        first("valid_to_n").alias("valid_to_n")
      )
      .as[WlanHotspotTypes.WlanHotspotStage]

    ret.groupBy("hotspot_country_code").count().collect().foreach(println(_))

    ret
  }

  private def joinWlanHotspotData(wlanHotspotNew: Dataset[WlanHotspotTypes.WlanHotspotStage], wlanHotspotOld: Dataset[WlanHotspotTypes.WlanHostpot], maxId: Long): DataFrame = {
    def getField(nameNew: String, nameOld: String): Column = {
      when(col(nameNew).isNotNull, col(nameNew)).otherwise(col(nameOld))
    }

    val toMerge = wlanHotspotOld.filter($"valid_to" >= lit(maxDate).cast(TimestampType) && $"wlan_hotspot_ident_code".startsWith(lit("undefined_OTHER_OTHER")))
    logger.info(s"Wlan hotspot to merge: ${toMerge.count()}")

    val oldHotspot = wlanHotspotOld.filter(!($"valid_to" >= lit(maxDate).cast(TimestampType) && $"wlan_hotspot_ident_code".startsWith(lit("undefined_OTHER_OTHER"))))
    logger.info(s"Wlan hotspot untouched data: ${oldHotspot.count()}")

    logger.info("Old Wlan data to be joined with new data from order_db")
    val merge = toMerge
      .join(wlanHotspotNew,
        $"wlan_hotspot_ident_code" === $"hotspot_ident_code", "full_outer")

    val toUnion = merge
      .filter($"wlan_hotspot_ident_code".isNotNull && ($"hotspot_ident_code".isNull || $"valid_to" < lit(maxDate).cast(TimestampType)))
      .drop("hotspot_timezone", "hotspot_venue_type_code", "hotspot_venue_code", "hotspot_provider_code", "hotspot_country_code", "hotspot_city_code", "hotspot_id", "valid_to_n", "valid_from_n", "hotspot_ident_code")
      .distinct()

    val furtherOn =
      merge
        .filter(!($"wlan_hotspot_ident_code".isNotNull && ($"hotspot_ident_code".isNull || $"valid_to" < lit(maxDate).cast(TimestampType))))
        .withColumn("wlan_hotspot_ident_code", getField("wlan_hotspot_ident_code", "hotspot_ident_code"))
        .withColumn("valid_from", getField("valid_from", "valid_from_n"))
        .withColumn("valid_to", getField("valid_to_n", "valid_to"))
        .withColumn("wlan_hotspot_timezone", getField("hotspot_timezone", "wlan_hotspot_timezone"))
        .withColumn("wlan_venue_type_code", getField("hotspot_venue_type_code", "wlan_venue_type_code"))
        .withColumn("wlan_venue_code", getField("hotspot_venue_code", "wlan_venue_code"))
        .withColumn("wlan_provider_code", getField("hotspot_provider_code", "wlan_provider_code"))
        .withColumn("country_code", getField("hotspot_country_code", "country_code"))
        .withColumn("city_code", getField("hotspot_city_code", "city_code"))
        .select(oldHotspot.columns.head, oldHotspot.columns.tail: _*)
        .distinct()

    furtherOn.printSchema()

    logger.info(s"Calculating new hotspotIDs based on maxId from previous runs ${maxId}")
    val forNewIDs = furtherOn
      .filter("wlan_hotspot_id is null")
      .select("wlan_hotspot_ident_code")
      .sort()
      .distinct()
      .withColumn("id", monotonically_increasing_id().cast(LongType))
      .withColumn("id", $"id" + lit(maxId))

    logger.info("Joining data with new IDs wiht old wlan hotspot data")
    val newGuys = furtherOn
      .join(forNewIDs, Seq("wlan_hotspot_ident_code"), "left_outer")
      .withColumn("wlan_hotspot_id", when($"wlan_hotspot_id".isNull, $"id").otherwise($"wlan_hotspot_id"))
      .drop("id")
      .select(oldHotspot.columns.head, oldHotspot.columns.tail: _*)
      .distinct()

    logger.info(s"unique newGuys count: ${newGuys.select("wlan_hotspot_ident_code").distinct().count()}")

    val testRun = newGuys.select("wlan_hotspot_ident_code").join(oldHotspot.select("wlan_hotspot_ident_code").distinct(), Seq("wlan_hotspot_ident_code"), "inner").count()

    logger.info(s"GUYS PRESENT IN THE OLD HOTSPOT ${testRun}")

    logger.info(s"New guys for the  wlan hotspot data count: ${newGuys.count()}")
    val ret = oldHotspot.toDF()
      .union(newGuys)
      .union(toUnion)
    ret.groupBy("country_code").count().collect().foreach(println(_))
    ret

  }

  private def mapVoucher(data: Dataset[OrderDBStructures.OrderDBInput]) = {
    data
      .filter(!($"result_code".notEqual(lit("OK")) || $"cancellation".isNotNull))
      .withColumn("tmp_username", split($"username", "@"))
      .withColumn("wlif_username", when($"username".contains("@"), lower(hex(sha2($"tmp_username".getItem(0), 224)))))
      .withColumn("wlif_realm_code", when($"username".contains("@"), $"tmp_username".getItem(1)))
      .withColumn("wlan_username", trim($"username"))
      .na.fill("#", Seq("wlan_username"))
      .withColumn("wlan_username", when($"wlan_username".equalTo(lit("#")), lit("#")).otherwise(regexp_replace(encoder3des(lit(encoderPath), $"wlan_username"), "[\n\r]", "")))
      .withColumn("wlan_ta_id", $"transaction_id")
      .withColumn("wlan_request_date", to_timestamp(concat($"transaction_date", $"transaction_time"), "yyyyMMddHHmmss"))
      .withColumn("year", year($"wlan_request_date"))
      .withColumn("month", month($"wlan_request_date"))
      .withColumn("day", dayofmonth($"wlan_request_date"))
  }

  def processData(): OderdDBPRocessingOutputs = {
    logger.info("Preparing input data - orderDB, wlanhotspot data, error codes and old error codes list file")
    val data = new OrderDBData(orderDb = orderDBInputData.inputMPS, oldErrorCodes = orderDBInputData.oldErrorCodes, inputHotspot = orderDBInputData.dataHotspot)
    logger.info("Reading and filtering input data")
    val wlanHotspotNew = mapWlanHotspotStage(data.fullData)
    logger.info("Preprocessing input data")
    val preprocessedWlanHotspot = preprocessWlanHotspotStage(wlanHotspotNew)
    logger.info("Reading old wlan hotspot data")
    val wlanHotspotOld = orderDBInputData.dataHotspot.as[WlanHotspotTypes.WlanHostpot]
    logger.info("Joining new wlan hotspot data with the old wlan hotspot set")
    val maxId = data.hotspotIDsSorted.first().getLong(0)
    val wlanData = joinWlanHotspotData(preprocessedWlanHotspot, wlanHotspotOld, maxId)
    logger.info("Generating outputs - wlan hotspot file and error code list file")

    OderdDBPRocessingOutputs(
      wlanHotspot = wlanData, //cptm_ta_d_wlan_hotspot
      errorCodes = data.allOldErrorCodes.union(data.newErrorCodes), //cptm_ta_d_wlan_error_code
      mapVoucher = mapVoucher(data.fullData), //cptm_ta_f_wlif_map_voucher
      orderDb = wlanHotspotNew.toDF(), //cptm_ta_f_wlan_orderdb
      maxHotspotID = maxId
    )
  }
}
