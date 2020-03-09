package com.tmobile.sit.ignite.inflight

import java.sql.Timestamp
import java.time.LocalDateTime

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.tmobile.sit.common.readers.{CSVMultifileReader, CSVReader, ExcelReader, MSAccessReader}
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{ExchangeRates, MapVoucher, OrderDB}
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes.Radius
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import com.tmobile.sit.ignite.inflight.datastructures.{InputStructures, InputTypes}
import com.tmobile.sit.ignite.inflight.processing.{AggregateRadiusCreditData, StageProcess}


@RunWith(classOf[JUnitRunner])
class TestStage extends FlatSpec with DataFrameSuiteBase {
  implicit lazy val _: SparkSession = spark
  val radius = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_radius.csv",
    header = false,
    schema = Some(InputStructures.radiusStructure),
    delimiter = "|",
    timestampFormat = "yyyy-MM-dd HH:mm:ss" )
    .read()

  val voucher = CSVReader("src/test/resources/data/cptm_ta_f_wlif_map_voucher.20200212.csv",
    header = false,
    schema = Some(InputStructures.mapVoucherStructure),
    delimiter = "|",
    timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
    dateFormat = "yyyy-MM-dd")
    .read().as[MapVoucher]

  val orderDB =  CSVReader("src/test/resources/data/cptm_ta_f_wlan_orderdb.20200212.csv",
    header = false,
    schema = Some(InputStructures.orderdbStructure),
    delimiter = "|",
    timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
    dateFormat = "yyyy-MM-dd")
    .read().as[OrderDB]

  val exchangeRates = CSVReader("src/test/resources/data/cptm_ta_t_exchange_rates.csv",
    header = false,
    schema = Some(InputStructures.exchangeRatesStructure),
    delimiter = "|",
    timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
    dateFormat = "yyyy-MM-dd")
    .read().as[ExchangeRates]

  val firstDate = Timestamp.valueOf("2019-02-10 00:00:00")
  val lastPlus1Date = Timestamp.valueOf("2020-02-28 00:00:00")
  val minRequestDate = Timestamp.valueOf("2011-02-11 00:00:00")



  "RadiusCreditData" should "be prepared properly" in {
    import spark.implicits._


    val stage = new StageProcess

    //radius.select(max("wlif_session_stop")).show(false)

    val radiusPrep= stage.processRadius(radius)

    //radiusPrep.summary().select("wlif_session_stop").show(true)

    val aggregatevoucher = new AggregateRadiusCreditData(radius = radiusPrep, voucher = voucher, orderDB = orderDB, exchangeRates = exchangeRates,
      firstDate = firstDate, lastPlus1Date =lastPlus1Date, minRequestDate = minRequestDate )

    aggregatevoucher.getExchangeRates.show(false)
  }

}
