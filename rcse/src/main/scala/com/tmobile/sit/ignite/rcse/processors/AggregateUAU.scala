package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.aggregateuau.AgregateUAUProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{AgregateUAUInputs, LookupsData}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class AggregateUAU(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

  def processData(): DataFrame = {

    val activeUsers = new AgregateUAUInputs()

    val lookups = new LookupsData()

    new AgregateUAUProcessor(activeUsers, lookups, settings.app.processingDate).result


    /*
    processor.result
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "true")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("dateFormat", "yyyyMMdd")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_x_rcse_uau_d.TMD.20200607.csv");
*/

  }

}
