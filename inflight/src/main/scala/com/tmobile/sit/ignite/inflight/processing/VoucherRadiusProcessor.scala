package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregVchrRadiusInterimData, AggregateVchrRdsExechangeRates, AggregateVoucherUsers}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData, NormalisedExchangeRates, StageData}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * this class wrapps outputs for voucher radius - dialy extract and full file
 * @param voucherRadiusDaily
 * @param voucherRadiusFull
 */

case class VoucherRadiusOutputs(voucherRadiusDaily: DataFrame, voucherRadiusFull: DataFrame)

/**
 * This class generates voucher radius data - daily and full file
 * @param stageData - staged input files - preprocessed input files
 * @param refData - reference data - orderDB, map voucher
 * @param normalisedExchageRates - exchange rates
 * @param firstDate - date for the report
 * @param lastPlus1Date - upper bound date for the calculation - not included in the result
 * @param minRequestDate - lower date bound for data to read
 * @param sparkSession - what a surprise ;)
 */
class VoucherRadiusProcessor(stageData: StageData, refData: ReferenceData, normalisedExchageRates: NormalisedExchangeRates,
                             firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp)
                            (implicit sparkSession: SparkSession) extends Logger {
  def getVchrRdsData() : VoucherRadiusOutputs = {
    logger.info("Preparing interim structures for voucherRadius aggregates")
    val interimData: AggregVchrRadiusInterimData = new AggregVchrRadiusInterimData(flightLeg = stageData.flightLeg, radius = stageData.radius,voucher=refData.voucher,
      orderDB = refData.orderDB, firstDate=firstDate, lastPlus1Date=lastPlus1Date)
    logger.info("preparing radiusVoucher aggregates Full file")
    val aggregateVoucherUsers = new AggregateVoucherUsers(interimData=interimData)
    logger.info("preparing radiusVoucher aggregates daily file")
    val aggregatesWithExchangeRates = new AggregateVchrRdsExechangeRates(interimData = interimData,normalisedExchangeRates = normalisedExchageRates)
    logger.info("RadiusVoucher aggregates ready")
    VoucherRadiusOutputs(voucherRadiusDaily = aggregatesWithExchangeRates.voucherRadiusDaily, voucherRadiusFull = aggregateVoucherUsers.vchrRadiusTFull)
  }
}
