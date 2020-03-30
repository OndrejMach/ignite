package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.ignite.inflight.config.OutputFiles
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class AggregatesWriter(radiusCredit: DataFrame,vchrRadiusData: VoucherRadiusOutputs, outputConf: OutputFiles )(implicit sparkSession: SparkSession) extends InflightWriter(outputConf.timestampFormat.get) {
  override def writeOutput(): Unit = {
    logger.info("Writing aggregates files")
    val translate = sparkSession.udf.register("translateSeconds",translateSeconds)


    writeOut(outputConf.path.get + outputConf.radiusCreditDailyFile.get, radiusCredit.withColumn("WLIF_SESSION_TIME",translate(col("WLIF_SESSION_TIME")) ))
    writeOut(outputConf.path.get + outputConf.vchrRadiusDailyFile.get, vchrRadiusData.voucherRadiusDaily.withColumn("WLIF_SESSION_TIME",translate(col("WLIF_SESSION_TIME"))))
    writeOut(outputConf.path.get + outputConf.voucherRadiusFile.get, vchrRadiusData.voucherRadiusFull)
  }
}
