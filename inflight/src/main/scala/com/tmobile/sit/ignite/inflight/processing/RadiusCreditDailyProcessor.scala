package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregateRadiusCredit, AggregateRadiusCreditData}
import com.tmobile.sit.ignite.inflight.processing.data.{NormalisedExchangeRates, ReferenceData, StageData}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Processor doing Radius credit daily aggregates
 * @param refData - reference (stage) data
 * @param stageData - stage data for inputs
 * @param normalisedExchangeRates - exchange rates normalised
 * @param firstDate - date for the data
 * @param lastPlus1Date - upper data bound - not in the output
 * @param minRequestDate - lower limit for data to be read
 * @param sparkSession - of course
 */

class RadiusCreditDailyProcessor(refData: ReferenceData, stageData: StageData, normalisedExchangeRates: NormalisedExchangeRates,
                                 firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp
                                 )(implicit sparkSession: SparkSession) extends Logger {

  def executeProcessing(): DataFrame = {

    val aggregateRadiusCredit = new AggregateRadiusCreditData(radius = stageData.radius, voucher = refData.voucher, orderDB = refData.orderDB,
      firstDate = firstDate, lastPlus1Date = lastPlus1Date, minRequestDate = minRequestDate)


    val processing = new AggregateRadiusCredit(aggregateRadiusCredit,normalisedExchangeRates)

    processing.executeProcessing()

  }
}
