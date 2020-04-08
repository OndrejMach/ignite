package com.tmobile.sit.ignite.inflight.processing.aggregates

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.data.NormalisedExchangeRates
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Creates Radiu Credit daily aggregates.
 *
 * @param data - pre-calculated inputs for final aggregation and joins
 * @param normalisedExchangeRates - exchange rates to calculate prices from other currencies than EUR
 * @param sparkSession - :)
 */

class AggregateRadiusCredit(data: AggregateRadiusCreditData, normalisedExchangeRates: NormalisedExchangeRates)(implicit sparkSession: SparkSession) extends Logger {
  private def aggregateRadiusVoucher(): DataFrame = {
    //data.filterAggrRadius.show(false)

    logger.debug(s"Voucher count: ${data.mapVoucher.select("wlif_username").distinct().count()}")
    logger.debug(s"Aggregated radius: ${data.filterAggrRadius.select("wlif_username").distinct().count()}")

    data.filterAggrRadius
      .drop("wlif_realm_code")
      .join(data.mapVoucher, Seq("wlif_username"), "inner")

  }

  private def joinWithOrderDB(radiusWithVoucher: DataFrame) = {
    radiusWithVoucher
      .join(data.filterOrderDB, radiusWithVoucher("wlan_username") === data.filterOrderDB("username"), "inner")

  }

  private def joinWithExchangeRates(withOrderDB: DataFrame) = {
    normalisedExchangeRates.joinWithExchangeRates(withOrderDB)
  }

  def executeProcessing(): DataFrame = {
    val translate = sparkSession.udf.register("translateSeconds", translateSeconds)

    //join radius with map voucher
    logger.debug(s"COUNT RADIUS AGGREGATED: ${data.filterAggrRadius.count()}")
    val radiusWithVoucher = aggregateRadiusVoucher()
    logger.debug(s"COUNT RADIUSWITHVOUCHER: ${radiusWithVoucher.count()}")
    //radiusWithVoucher.show(false)
    //join with orderDB
    val withOrderDB = joinWithOrderDB(radiusWithVoucher)
    logger.debug(s"COUNT RADIUSWITHVOUCHER with ORDERDB: ${withOrderDB.count()}")
    //withOrderDB.show(false)
    //joinWithExchangeRates
    val withExRts = joinWithExchangeRates(withOrderDB)
      .withColumnRenamed("count_sessions", "wlif_num_sessions")
    logger.debug(s"COUNT RADIUSWITHVOUCHER with ORDERDB with ExchangeRates: ${withExRts.count()}")

    withExRts
      .withColumn("wlif_session_time", translate(col("wlif_session_time")))
      .withColumn("wlif_session_volume", round(col("wlif_session_volume"), 2))
      .withColumn("amount_incl_vat", round(col("amount_incl_vat"), 2))
      .withColumn("amount_excl_vat", round(col("amount_excl_vat"), 2))
  }

}
