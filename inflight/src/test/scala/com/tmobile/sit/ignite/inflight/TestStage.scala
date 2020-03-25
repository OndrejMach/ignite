package com.tmobile.sit.ignite.inflight

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.tmobile.sit.ignite.inflight.processing.data.{ReferenceData, StageData}
import com.tmobile.sit.ignite.inflight.processing.aggregates.AggregateRadiusVoucherData
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TestStage extends FlatSpec with DataFrameSuiteBase {
  implicit lazy val _: SparkSession = spark


  val firstDate = Timestamp.valueOf("2019-02-10 00:00:00")
  val lastPlus1Date = Timestamp.valueOf("2020-02-28 00:00:00")
  val minRequestDate = Timestamp.valueOf("2011-02-11 00:00:00")



  "RadiusCreditData" should "be prepared properly" in {


    val stage = new StageData

    //radius.select(max("wlif_session_stop")).show(false)

    val radiusPrep= stage.radius(StageData.radius)

    //radiusPrep.summary().select("wlif_session_stop").show(true)

    val aggregatevoucher = new AggregateRadiusVoucherData(radius = radiusPrep, voucher = StageData.voucher, orderDB = StageData.orderDB, exchangeRates = StageData.exchangeRates,
      firstDate = firstDate, lastPlus1Date =lastPlus1Date, minRequestDate = minRequestDate )

    assert(aggregatevoucher.getExchangeRates.count() > 0)
    assert(aggregatevoucher.filterAggrRadius.count() >0)
    assert(aggregatevoucher.mapVoucher.count() >0)
    assert(aggregatevoucher.filterOrderDB.count() >0)
  }

}
