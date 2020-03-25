package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregateRadiusCredit, AggregateRadiusVoucherData}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData, StageData}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RadiusCreditDailyProcessor(refData: ReferenceData, stageData: StageData,
                                 firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp
                                 )(implicit sparkSession: SparkSession, runId: Int, loadDate: Timestamp) extends Processor {

  def executeProcessing(): DataFrame = {

    val aggregatevoucher = new AggregateRadiusVoucherData(radius = stageData.radius, voucher = refData.voucher, orderDB = refData.orderDB, exchangeRates = refData.exchangeRates,
      firstDate = firstDate, lastPlus1Date = lastPlus1Date, minRequestDate = minRequestDate)


    val processing = new AggregateRadiusCredit(aggregatevoucher)

    processing.executeProcessing()
  }
}
