package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregVchrRadiusInterimData, AggregateVoucherUsers, AggregateWithExechangeRates}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData, StageData}
import org.apache.spark.sql.DataFrame

case class VoucherRadiusOutputs(voucherRadiusDaily: DataFrame, voucherRadiusFull: DataFrame)

class VoucherRadiusProcessor(stageData: StageData, refData: ReferenceData,
                             firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp)
                            (implicit runId : Int, loadDate: Timestamp) extends Logger {
  def getVchrRdsData() : VoucherRadiusOutputs = {
    import TransformDataFrameColumns.TransformColumnNames
    logger.info("Preparing interim structures for voucherRadius aggregates")
    val interimData: AggregVchrRadiusInterimData = new AggregVchrRadiusInterimData(flightLeg = stageData.flightLeg, radius = stageData.radius,voucher=refData.voucher,
      orderDB = refData.orderDB, firstDate=firstDate, lastPlus1Date=lastPlus1Date)
    logger.info("preparing radiusVoucher aggregates Full file")
    val aggregateVoucherUsers = new AggregateVoucherUsers(interimData=interimData)
    logger.info("preparing radiusVoucher aggregates daily file")
    val aggregatesWithExchangeRates = new AggregateWithExechangeRates(interimData = interimData, exchangeRates = refData.exchangeRates, minDate = minRequestDate)
    logger.info("RadiusVoucher aggregates ready")
    VoucherRadiusOutputs(voucherRadiusDaily = aggregatesWithExchangeRates.voucherRadiusDaily.columnsToUpperCase(), voucherRadiusFull = aggregateVoucherUsers.vchrRadiusTFull.columnsToUpperCase())
  }
}
