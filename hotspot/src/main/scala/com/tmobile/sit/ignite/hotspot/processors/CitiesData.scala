package com.tmobile.sit.ignite.hotspot.processors

import java.sql.Date

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, max, monotonically_increasing_id, upper}
import org.apache.spark.sql.types.TimestampType

class CitiesData(wlanAndOrderDBData: DataFrame, oldCitieData: DataFrame)(implicit processingDate: Date) extends Logger {

  private val cityData = {
    logger.info("Preparing old cities data")
    oldCitieData
      .sort("city_id")
  }

  private val maxCityId = {
    logger.info("Calculating actual max city_id")
    cityData.select(max("city_id")).first().getLong(0)
  }

  private val newCities = {
    logger.info("Getting new cities from hotspot and orderDB data")
    wlanAndOrderDBData
      .select("city_code")
      .distinct()
      .join(cityData, Seq("city_code"), "left_outer")
      .filter(col("city_id").isNull && col("city_code").isNotNull)
      .withColumn("city_id", monotonically_increasing_id() + lit(maxCityId))
      .withColumn("city_desc", upper(col("city_code")))
      .withColumn("city_ldesc", lit("new"))
      .withColumn("load_date", lit(processingDate).cast(TimestampType))
      .withColumn("entry_id", lit(1))
      .select("city_id", "city_code", "city_desc", "city_ldesc","entry_id","load_date")
  }

  val allCities = {
    logger.info(s"Preparing new cities data, new cities count: ${newCities.count()}")
    newCities
      .union(cityData)
      //.select("city_id", "city_code", "city_desc", "city_ldesc")
  }
}
