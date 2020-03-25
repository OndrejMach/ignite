package com.tmobile.sit.ignite.inflight
import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregateRadiusCredit, AggregateRadiusCreditData}
import com.tmobile.sit.ignite.inflight.processing.data.{ReferenceData, StageData}
import com.tmobile.sit.ignite.inflight.processing.AggregateRadiusCreditData
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TestRadiusCreditProcessing extends FlatSpec with DataFrameSuiteBase {
  implicit lazy val _: SparkSession = spark


  val firstDate = Timestamp.valueOf("2019-02-10 00:00:00")
  val lastPlus1Date = Timestamp.valueOf("2020-02-28 00:00:00")
  val minRequestDate = Timestamp.valueOf("2011-02-11 00:00:00")


  "RadiusCreditData" should "be processed well" in {


    val stage = new StageData

    //radius.select(max("wlif_session_stop")).show(false)

    val radiusPrep= stage.radius(StageData.radius)

    //radiusPrep.summary().select("wlif_session_stop").show(true)

    val aggregatevoucher = new AggregateRadiusCreditData(radius = radiusPrep, voucher = StageData.voucher, orderDB = StageData.orderDB, exchangeRates = StageData.exchangeRates,
      firstDate = firstDate, lastPlus1Date =lastPlus1Date, minRequestDate = minRequestDate )


    val processing = new  AggregateRadiusCredit(aggregatevoucher, loadDate = Timestamp.valueOf("2020-03-10 00:00:00"), runId = 123)

    processing.executeProcessing().show(false)

  }
}
