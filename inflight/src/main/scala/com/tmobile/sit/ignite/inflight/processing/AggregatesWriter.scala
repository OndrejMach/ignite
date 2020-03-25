package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.ignite.inflight.config.OutputFiles
import org.apache.spark.sql.{DataFrame, SparkSession}

class AggregatesWriter(radiusCredit: DataFrame,vchrRadiusData: VoucherRadiusOutputs, outputConf: OutputFiles )(implicit sparkSession: SparkSession) extends InflightWriter(outputConf.timestampFormat.get) {
  override def writeOutput(): Unit = {
    logger.info("Writing aggregates files")
    writeOut(outputConf.path.get + outputConf.radiusCreditDailyFile.get, radiusCredit)
    writeOut(outputConf.path.get + outputConf.vchrRadiusDailyFile.get, vchrRadiusData.voucherRadiusDaily)
    writeOut(outputConf.path.get + outputConf.voucherRadiusFile.get, vchrRadiusData.voucherRadiusFull)
  }
}
