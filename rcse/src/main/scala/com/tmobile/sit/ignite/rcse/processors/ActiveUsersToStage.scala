package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inputs.ActiveUsersInputs
import org.apache.spark.sql.{SaveMode, SparkSession}

class ActiveUsersToStage(settings: Settings, processingDate: Date)(implicit sparkSession: SparkSession) extends Processor {

  override def processData(): Unit = {
    val inputData = new ActiveUsersInputs(settings)

    val processor = new activeusers.ActiveUsersProcessor(inputData, processingDate)

      processor.result
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


  }
}
