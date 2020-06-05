package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.inflight.datastructures.InputStructures
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{ExchangeRates, MapVoucher, OrderDB}
import org.apache.spark.sql.SparkSession

object InputData {
  def radius(implicit sparkSession: SparkSession) = {
    CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_radius.csv",
      header = false,
      schema = Some(InputStructures.radiusStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )
      .read()
  }

  def voucher(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("src/test/resources/data/cptm_ta_f_wlif_map_voucher.20200212.csv",
      header = false,
      schema = Some(InputStructures.mapVoucherStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
      dateFormat = "yyyy-MM-dd")
      .read().as[MapVoucher]
  }

  def orderDB(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("src/test/resources/data/cptm_ta_f_wlan_orderdb.20200212.csv",
      header = false,
      schema = Some(InputStructures.orderdbStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      dateFormat = "yyyy-MM-dd")
      .read().as[OrderDB]
  }

  def exchangeRates(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    CSVReader("src/test/resources/data/cptm_ta_t_exchange_rates.csv",
      header = false,
      schema = Some(InputStructures.exchangeRatesStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      dateFormat = "yyyy-MM-dd")
      .read().as[ExchangeRates]
  }

}
