package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.ignite.inflight.config.OutputFiles
import com.tmobile.sit.ignite.inflight.datastructures.OutputStructure
import com.tmobile.sit.ignite.inflight.processing.{TransformDataFrameColumns, VoucherRadiusOutputs}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AggregatesWriter(radiusCredit: DataFrame,vchrRadiusData: VoucherRadiusOutputs, outputConf: OutputFiles )(implicit sparkSession: SparkSession) extends InflightWriter(outputConf.timestampFormat.get) {
  override def writeOutput(): Unit = {
    logger.info("Writing aggregates files")

    import TransformDataFrameColumns.TransformColumnNames

    writeOut(outputConf.path.get + outputConf.radiusCreditDailyFile.get, radiusCredit.select(OutputStructure.radiusCreditDailyColumns.head, OutputStructure.radiusCreditDailyColumns.tail :_*).columnsToUpperCase())
    writeOut(outputConf.path.get + outputConf.vchrRadiusDailyFile.get, vchrRadiusData.voucherRadiusDaily.select(OutputStructure.voucherRadiusDailyColumns.head, OutputStructure.voucherRadiusDailyColumns.tail :_*).columnsToUpperCase())
    writeOut(outputConf.path.get + outputConf.voucherRadiusFile.get, vchrRadiusData.voucherRadiusFull.select(OutputStructure.voucherRadiusFullColumns.head, OutputStructure.voucherRadiusFullColumns.tail :_*).columnsToUpperCase())
  }
}
