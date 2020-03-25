package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregateRadiusCredit, AggregateRadiusCreditData}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData, StageData}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RadiusVoucheProcessor(refData: ReferenceData, stageData: StageData,
                            firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp,
                            runId: Int, loadDate: Timestamp)(implicit sparkSession: SparkSession) extends Processor {

  def executeProcessing(): DataFrame = {

    val aggregatevoucher = new AggregateRadiusCreditData(radius = stageData.radius, voucher = refData.voucher, orderDB = refData.orderDB, exchangeRates = refData.exchangeRates,
      firstDate = firstDate, lastPlus1Date = lastPlus1Date, minRequestDate = minRequestDate)


    val processing = new AggregateRadiusCredit(aggregatevoucher, loadDate = loadDate, runId = runId)

    processing.executeProcessing()
  }
}
