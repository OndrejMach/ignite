package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.aggregateuau.AgregateUAUProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.AgregateUAUInputs
import com.tmobile.sit.ignite.rcse.structures.{CommonStructures, Terminal}
import com.tmobile.sit.ignite.rcse.processors.inputs.LookupsData
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

class AggregateUAU(time_key: Date, settings: Settings)(implicit sparkSession: SparkSession) extends Processor {

  import sparkSession.implicits._

  override def processData(): Unit = {

    val activeUsers = new AgregateUAUInputs(settings)

    val lookups = new LookupsData(settings)

    val processor = new AgregateUAUProcessor(activeUsers, lookups, time_key)

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


  }

}
