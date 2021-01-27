package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsDataReader
import com.tmobile.sit.ignite.rcse.writer.RCSEOutputs
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.DataFrame

object TransformDataFrameColumns {
  implicit class TransformColumnNames(df : DataFrame) {
    def columnsToUpperCase() : DataFrame = {
      df.toDF(df.columns.map(_.toUpperCase()):_*)
    }
  }
}

/**
 * this class processes outputs - simply reads stage files used for output generation, transforms column names to upper case (using the implicit class above)
 * and all done
 * @param sparkSession
 * @param settings - paths where to read required stage files
 */
class OutputsProcessor(implicit sparkSession: SparkSession, settings: Settings) {
  def getData:RCSEOutputs = {
    val lookups = new LookupsDataReader()

    import TransformDataFrameColumns.TransformColumnNames

    RCSEOutputs(
      terminal = lookups.terminal.columnsToUpperCase(),
      terminalSW = lookups.terminalSW.columnsToUpperCase(),
      client = lookups.client.columnsToUpperCase(),
      activeUser = sparkSession.read.parquet(s"${settings.stage.activeUsers}/date=${settings.app.processingDate}").columnsToUpperCase(),
      initConf = sparkSession.read.parquet(s"${settings.stage.initConf}/date=${settings.app.processingDate}").columnsToUpperCase(),
      initUser = sparkSession.read.parquet(s"${settings.stage.initUser}/date=${settings.app.processingDate}").columnsToUpperCase(),
      uau = sparkSession.read.parquet(s"${settings.stage.uauFile}/date=${settings.app.processingDate}").columnsToUpperCase()
    )
  }
}
