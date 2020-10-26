package com.tmobile.sit.ignite.inflight.processing.aggregates

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * This class calculate voucher radius full file
 * @param interimData - required input data preprocessed - common with  voucher radius daily processing
 */

class AggregateVoucherUsers(interimData: AggregVchrRadiusInterimData) extends Logger {


  val vchrRadiusTFull: DataFrame = {
    logger.info(s"T table input count: ${interimData.joinVoucherWithRadiusFlightLeg.count()}")
    logger.info("Joining Voucher with Radius and FligheLeg to get non-voucher users")
    val nonVoucher = interimData.joinVoucherWithRadiusFlightLeg.filter("wlan_ta_id is null")
    logger.info("Getting voucher users")
    val voucher = interimData.joinVoucherWithRadiusFlightLeg.filter("wlan_ta_id is not null")

    logger.info(s"Counts on T table: nonVoucher - ${nonVoucher.count()} voucher - ${voucher.count()}")

    logger.info("Aggregating non-Voucher users")
    val nonVoucherAggr = nonVoucher
      .groupBy("wlif_flight_id", "wlif_date_time_closed", "wlif_airline_code", "wlif_account_type")
      .agg(
        sum("count_sessions").alias("non_voucher_sessions"),
        count("wlif_flight_id").alias("non_voucher_users"),
        first("wlif_flight_number").alias("wlif_flight_number"),
        first("wlif_date_time_opened").alias("wlif_date_time_opened"),
        first("wlif_num_users").alias("wlif_num_users"),
        first("wlif_num_sessions").alias("wlif_num_sessions"),
        first("wlif_realm_code").alias("wlif_realm_code"),
        first("wlan_hotspot_ident_code").alias("wlan_hotspot_ident_code")
      )
    logger.info("Aggregating voucher users")
    val voucherAggr = voucher
      .groupBy("wlif_flight_id", "wlif_date_time_closed", "wlif_airline_code", "wlif_account_type")
      .agg(
        sum("count_sessions").alias("voucher_sessions"),
        count("wlif_flight_id").alias("voucher_users"),
        first("wlif_num_users").alias("vchr_wlif_num_users"),
        first("wlif_num_sessions").alias("vchr_wlif_num_sessions")
      )
      .withColumnRenamed("wlif_account_type", "vchr_wlif_account_type")
    logger.info(s"Counts on T table aggregates: nonVoucher - ${nonVoucherAggr.count()} voucher - ${voucherAggr.count()}")

    logger.info("Joining voucher and non-voucher aggregates")
    voucherAggr
      .join(nonVoucherAggr, Seq("wlif_flight_id", "wlif_date_time_closed", "wlif_airline_code"), "right")
      .na
      .fill(0, Seq("vchr_voucher_sessions", "vchr_voucher_users", "vchr_wlif_num_users", "vchr_wlif_num_sessions"))
      .withColumn("flight_users", when(col("vchr_wlif_num_users").isNull && col("wlif_num_users") === lit(-1),
        col("voucher_users") + col("non_voucher_users"))
        .otherwise(col("wlif_num_users")))
      .withColumn("flight_users", when(col("wlif_num_users").isNull && col("vchr_wlif_num_users") === lit(-1),
        col("voucher_users") + col("non_voucher_users"))
        .otherwise(col("vchr_wlif_num_users")))

      .withColumn("flight_sessions", when(col("vchr_wlif_num_sessions").isNull && col("wlif_num_sessions") === lit(-1),
        col("voucher_sessions") + col("non_voucher_sessions"))
        .otherwise(col("wlif_num_sessions")))
      .withColumn("flight_sessions", when(col("wlif_num_sessions").isNull && col("vchr_wlif_num_sessions") === lit(-1),
        col("voucher_sessions") + col("non_voucher_sessions"))
        .otherwise(col("vchr_wlif_num_sessions")))
  }

}