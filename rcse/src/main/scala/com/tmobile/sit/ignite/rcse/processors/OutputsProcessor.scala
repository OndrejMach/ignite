package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsDataReader
import com.tmobile.sit.ignite.rcse.writer.RCSEOutputs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.functions.{lit, sum}

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
class OutputsProcessor(implicit sparkSession: SparkSession, settings: Settings) extends Logger{
  def initUserpostProcessing(data:DataFrame) = {
    import sparkSession.implicits._
    logger.info("Running InitUser post processing")
    val unchanged = data
      .filter($"RCSE_REG_USERS_ALL".cast(LongType) > lit(5))
      .select("DATE_ID","NATCO_CODE","RCSE_INIT_CLIENT_ID","RCSE_INIT_TERMINAL_ID","RCSE_INIT_TERMINAL_SW_ID","RCSE_REG_USERS_NEW","RCSE_REG_USERS_ALL")
    val toAggregate = data.
      filter(($"RCSE_REG_USERS_ALL".cast(LongType) <= 5) or $"RCSE_REG_USERS_ALL".isNull)
      .na.fill(0, Seq("RCSE_REG_USERS_ALL", "RCSE_REG_USERS_NEW"))
    val aggregated = toAggregate
      .groupBy("DATE_ID", "NATCO_CODE")
      .agg(sum("RCSE_REG_USERS_NEW").alias("RCSE_REG_USERS_NEW"), sum("RCSE_REG_USERS_ALL").alias("RCSE_REG_USERS_ALL"))
      .withColumn("RCSE_INIT_CLIENT_ID", lit("-999"))
      .withColumn("RCSE_INIT_TERMINAL_ID", lit("-999"))
      .withColumn("RCSE_INIT_TERMINAL_SW_ID", lit("-999"))
      .select("DATE_ID","NATCO_CODE","RCSE_INIT_CLIENT_ID","RCSE_INIT_TERMINAL_ID","RCSE_INIT_TERMINAL_SW_ID","RCSE_REG_USERS_NEW","RCSE_REG_USERS_ALL")
    unchanged.union(aggregated)
  }


  def getData:RCSEOutputs = {
    val lookups = new LookupsDataReader()

    import TransformDataFrameColumns.TransformColumnNames

    RCSEOutputs(
      terminal = lookups.terminal.columnsToUpperCase(),
      terminalSW = lookups.terminalSW.columnsToUpperCase().select("RCSE_TERMINAL_SW_ID", "RCSE_TERMINAL_SW_DESC"),
      client = lookups.client.columnsToUpperCase(),
      activeUser = sparkSession.read.parquet(s"${settings.stage.activeUsers}/date=${settings.app.processingDate}").columnsToUpperCase(),
      initConf = sparkSession.read.parquet(s"${settings.stage.initConf}/date=${settings.app.processingDate}").columnsToUpperCase(),
      initUser = initUserpostProcessing(sparkSession.read.parquet(s"${settings.stage.initUser}/date=${settings.app.processingDate}").columnsToUpperCase()),
      uau = sparkSession.read.parquet(s"${settings.stage.uauFile}/date=${settings.app.processingDate}").columnsToUpperCase()
    )
  }
}
