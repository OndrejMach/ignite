package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inputs.ActiveUsersInputs
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class ActiveUsersToStage(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

  def processData(): DataFrame = {
    val inputData = new ActiveUsersInputs()

    new activeusers.ActiveUsersProcessor(inputData, settings.app.processingDate).result
/*
      result
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "false")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_f_rcse_active_user.TMD.20200607.csv");


 */

  }
}
