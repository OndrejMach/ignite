package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.inflight.config.{OutputFiles, StageFiles}
import com.tmobile.sit.ignite.inflight.datastructures.OutputStructure
import com.tmobile.sit.ignite.inflight.processing.{TransformDataFrameColumns, VoucherRadiusOutputs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Writer class for aggregated outputs - radiusCredit, voucher radius full and daily
 *
 * @param radiusCredit   - radiusCredit aggregates data
 * @param vchrRadiusData - voucher radius aggregates
 * @param outputConf     - output file configuration
 * @param sparkSession   - no comment ;)
 */

class AggregatesWriter(radiusCredit: DataFrame, vchrRadiusData: VoucherRadiusOutputs, outputConf: OutputFiles, stageConfig: StageFiles, airlines: Seq[String])(implicit sparkSession: SparkSession) extends InflightWriterUTF8Char(outputConf.timestampFormat.get) {
  private def writeTFileFull(): Unit = {
    logger.info(s"Merging stage files: ${stageConfig.path.get + stageConfig.tFileMask.get}")
    val oldData = CSVReader(path = stageConfig.path.get + stageConfig.tFileMask.get, header = false, delimiter = "|").read()
    logger.info(s"Writing resulting T file")
    writeData(outputConf.path.get + outputConf.voucherRadiusFile.get,
      //TODO this needs to be removed after deployment
      oldData
        .filter(col("_c5").isin(airlines: _*)).distinct()
        .drop("_c14")
        .drop("_c15"))
    //------
  }


  override def writeOutput(): Unit = {
    logger.info("Writing aggregates files")

    import TransformDataFrameColumns.TransformColumnNames

    writeData(outputConf.path.get + outputConf.radiusCreditDailyFile.get, radiusCredit.select(OutputStructure.radiusCreditDailyColumns.head, OutputStructure.radiusCreditDailyColumns.tail: _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.vchrRadiusDailyFile.get, vchrRadiusData.voucherRadiusDaily.select(OutputStructure.voucherRadiusDailyColumns.head, OutputStructure.voucherRadiusDailyColumns.tail: _*).columnsToUpperCase())
    writeData(stageConfig.path.get + stageConfig.tFileStage.get, vchrRadiusData.voucherRadiusFull.select(OutputStructure.voucherRadiusFullColumns.head, OutputStructure.voucherRadiusFullColumns.tail: _*).columnsToUpperCase(), writeHeader = false)
    writeTFileFull()

  }
}
