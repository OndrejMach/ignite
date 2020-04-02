package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregateRadiusCredit, AggregateRadiusCreditData}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData, StageData}
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class RadiusCreditDailyProcessor(refData: ReferenceData, stageData: StageData,
                                 firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp
                                 )(implicit sparkSession: SparkSession, runId: Int, loadDate: Timestamp) extends Logger {

  def executeProcessing(): DataFrame = {

    val aggregateRadiusCredit = new AggregateRadiusCreditData(radius = stageData.radius, voucher = refData.voucher, orderDB = refData.orderDB, exchangeRates = refData.exchangeRates,
      firstDate = firstDate, lastPlus1Date = lastPlus1Date, minRequestDate = minRequestDate)


    val processing = new AggregateRadiusCredit(aggregateRadiusCredit)

    processing.executeProcessing()

  }
}
