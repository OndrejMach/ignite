package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.hotspot.data.FailedTransactionsDataStructures
import com.tmobile.sit.ignite.hotspot.processors.udfs.DirtyStuff
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/**
 * this class gets all known vouchers from the data
 * @param wlanOrderDBExchangeRatesdata - info about transactions enriched with exchange rates
 * @param oldVoucherData - currently known vouchers.
 */
class VoucherData(wlanOrderDBExchangeRatesdata: DataFrame, oldVoucherData: DataFrame) extends  Logger{
  private val voucherData = {
    logger.info("Preparing old voucher data")
    oldVoucherData
      .withColumnRenamed("tmo_country_code", "natco")
      .withColumnRenamed("price", "amount")
      .withColumnRenamed("wlan_voucher_desc", "voucher_type")
      .drop("entry_id", "load_date")
    .sort("natco", "voucher_type", "amount", "duration")
  }

  private val maxVoucherID = {
    logger.info("Calculating actual maxVoucher ID")
    voucherData.select(max("wlan_voucher_id")).first().getLong(0)
  }

  private val newVouchers = {
    logger.info("Getting new vouchers from WlanHotspot and OrderDB data")
    wlanOrderDBExchangeRatesdata
      .sort(FailedTransactionsDataStructures.KEY_COLUMNS_VOUCHER.head, FailedTransactionsDataStructures.KEY_COLUMNS_VOUCHER.tail: _*)
      .dropDuplicates(FailedTransactionsDataStructures.KEY_COLUMNS_VOUCHER.head, FailedTransactionsDataStructures.KEY_COLUMNS_VOUCHER.tail: _*)
      .select(FailedTransactionsDataStructures.COLUMNS_VOUCHER.head, FailedTransactionsDataStructures.COLUMNS_VOUCHER.tail: _*)
      .join(voucherData.select("wlan_voucher_id", FailedTransactionsDataStructures.JOIN_COLUMNS_VOUCHER: _*), Seq("natco", "voucher_type", "amount", "duration"), "left_outer")
      .filter(col("wlan_voucher_id").isNull)
      .withColumn("row_nr", row_number.over(Window.orderBy("voucher_type")))
      .withColumn("wlan_voucher_id", expr(s"$maxVoucherID + row_nr"))
      .drop("row_nr")
      //.withColumn("wlan_voucher_id", monotonically_increasing_id() + lit(maxVoucherID))
      .select("wlan_voucher_id", FailedTransactionsDataStructures.COLUMNS_VOUCHER: _*)
  }

  val allVouchers= {
    logger.info("Merging new vouchers with the old ones, assigning new voucher IDs")
    newVouchers
      .union(voucherData.select("wlan_voucher_id", FailedTransactionsDataStructures.COLUMNS_VOUCHER: _*))
      .withColumn("valid_to", when(col("valid_to").isNull, lit(com.tmobile.sit.ignite.hotspot.data.FUTURE).cast(TimestampType)).otherwise(col("valid_to")))
  }

  val allVouchersForPrint = {

    val remove0s = udf {n: Double => DirtyStuff.removeTrailing0s(n)}

    logger.info("Preparing vouchers for output")
    allVouchers
      .withColumn("amount",remove0s(col("amount")) )
      .withColumn("conversion",remove0s(col("conversion")))
      .withColumnRenamed("voucher_type", "wlan_voucher_desc")
      .withColumnRenamed("natco","tmo_country_code")
      .withColumnRenamed("amount","price")
  }

}
