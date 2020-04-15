package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.ignite.inflight.config.OutputFiles
import com.tmobile.sit.ignite.inflight.datastructures.OutputStructure
import com.tmobile.sit.ignite.inflight.processing.{TransformDataFrameColumns, VoucherRadiusOutputs}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Writer class for aggregated outputs - radiusCredit, voucher radius full and daily
 * @param radiusCredit - radiusCredit aggregates data
 * @param vchrRadiusData - voucher radius aggregates
 * @param outputConf - output file configuration
 * @param sparkSession - no comment ;)
 */

class AggregatesWriter(radiusCredit: DataFrame,vchrRadiusData: VoucherRadiusOutputs, outputConf: OutputFiles )(implicit sparkSession: SparkSession) extends InflightWriterUTF8Char(outputConf.timestampFormat.get) {
  override def writeOutput(): Unit = {
    logger.info("Writing aggregates files")

    import TransformDataFrameColumns.TransformColumnNames

    writeData(outputConf.path.get + outputConf.radiusCreditDailyFile.get, radiusCredit.select(OutputStructure.radiusCreditDailyColumns.head, OutputStructure.radiusCreditDailyColumns.tail :_*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.vchrRadiusDailyFile.get, vchrRadiusData.voucherRadiusDaily.select(OutputStructure.voucherRadiusDailyColumns.head, OutputStructure.voucherRadiusDailyColumns.tail :_*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.voucherRadiusFile.get, vchrRadiusData.voucherRadiusFull.select(OutputStructure.voucherRadiusFullColumns.head, OutputStructure.voucherRadiusFullColumns.tail :_*).columnsToUpperCase())
  }
}
