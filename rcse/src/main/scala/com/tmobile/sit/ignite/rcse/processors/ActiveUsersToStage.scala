package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.readers.CSVReader
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

class ActiveUsersToStage(processingDate: Date)(implicit sparkSession: SparkSession) extends Processor {
  import sparkSession.implicits._

  override def processData(): Unit = {
    val regDerSchema = StructType(
      Seq(
        StructField("date_id", DateType, true),
        StructField("natco_code", StringType, true),
        StructField("msisdn", StringType, true),
        StructField("imsi", StringType, true),
        StructField("rcse_event_type", StringType, true),
        StructField("rcse_subscribed_status_id", IntegerType, true),
        StructField("rcse_active_status_id", IntegerType, true),
        StructField("rcse_tc_status_id", IntegerType, true),
        StructField("tac_code", StringType, true),
        StructField("rcse_version", StringType, true),
        StructField("rcse_client_id", IntegerType, true),
        StructField("rcse_terminal_id", IntegerType, true),
        StructField("rcse_terminal_sw_id", IntegerType, true),
        StructField("entry_id", IntegerType, true),
        StructField("load_date", TimestampType, true)
      )
    )

    val confSchema = StructType(
      Seq(
        StructField("date_id", DateType, true),
        StructField("natco_code", StringType, true),
        StructField("msisdn", StringType, true),
        StructField("rcse_tc_status_id", IntegerType, true),
        StructField("rcse_init_client_id", IntegerType, true),
        StructField("rcse_init_terminal_id", IntegerType, true),
        StructField("rcse_init_terminal_sw_id", IntegerType, true),
        StructField("rcse_curr_client_id", IntegerType, true),
        StructField("rcse_curr_terminal_id", IntegerType, true),
        StructField("rcse_curr_terminal_sw_id", IntegerType, true),
        StructField("modification_date", DateType, true),
        StructField("entry_id", IntegerType, true),
        StructField("load_date", TimestampType, true)
      )
    )

    val activeUsersSchema = StructType(
      Seq(
        StructField("date_id", DateType, true),
        StructField("natco_code", StringType, true),
        StructField("msisdn", StringType, true),
        StructField("rcse_tc_status_id", IntegerType, true),
        StructField("rcse_curr_client_id", IntegerType, true),
        StructField("rcse_curr_terminal_id", IntegerType, true),
        StructField("rcse_curr_terminal_sw_id", IntegerType, true),
        StructField("entry_id", IntegerType, true),
        StructField("load_date", TimestampType, true)
      )
    )

    val inputEvents = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_events.TMD.20200607.reg_der.csv",
      delimiter = "|",
      header = false,
      schema = Some(regDerSchema)
    ).read()

    val inputConf = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      delimiter = "|",
      header = false,
      schema = Some(confSchema)
    )
      .read()
      .select("msisdn", "rcse_tc_status_id", "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id")

    val activeUsersYesterday = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_active_user.TMD.20200606.csv",
      delimiter = "|",
      header = false,
      schema = Some(activeUsersSchema)
    ).read()

    val eventsYesterday = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_events.TMD.20200606.reg_der.csv",
      delimiter = "|",
      header = false,
      schema = Some(regDerSchema)
    ).read()


    val prepEvents = inputEvents
      .select("date_id", "natco_code", "msisdn")
      .sort("msisdn")
      .groupBy("msisdn")
      .agg(first("date_id").as("date_id"), first("natco_code").as("natco_code"))


    val join1 = prepEvents
      .join(inputConf, Seq("msisdn"), "left")
      .na
      .fill(-999, Seq("rcse_tc_status_id", "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id"))
      .select(
        "date_id", "natco_code",
        "msisdn", "rcse_tc_status_id",
        "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id"
      )

    val deregisteredEvents = eventsYesterday
      .filter($"rcse_event_type" === lit("DER"))
      .select("msisdn")
      .withColumn("deregistered", lit(1))

      val join2 = activeUsersYesterday
      .join(deregisteredEvents, Seq("msisdn"), "left")
      .filter($"deregistered".isNull)

   join1.printSchema()
    join2.printSchema()

    val join2Cols = join2.columns.map(_+"_yesterday")
    val result = join1
      .join(join2.toDF(join2Cols :_*), $"msisdn" === $"msisdn_yesterday", "outer")
      .withColumn("date_id", when($"date_id".isNull, lit(processingDate)).otherwise($"date_id"))
      .withColumn("natco_code", lit("TMD"))
      .withColumn("msisdn", when($"msisdn".isNull, $"msisdn_yesterday").otherwise($"msisdn"))
      .withColumn("rcse_tc_status_id", when($"rcse_tc_status_id".isNull, $"rcse_tc_status_id_yesterday").otherwise($"rcse_tc_status_id"))
      .withColumn("rcse_curr_client_id", when($"rcse_curr_client_id".isNull, $"rcse_curr_client_id_yesterday").otherwise($"rcse_curr_client_id"))
      .withColumn("rcse_curr_terminal_id", when($"rcse_curr_terminal_id".isNull, $"rcse_curr_terminal_id_yesterday").otherwise($"rcse_curr_terminal_id"))
      .withColumn("rcse_curr_terminal_sw_id", when($"rcse_curr_terminal_sw_id".isNull, $"rcse_curr_terminal_sw_id_yesterday").otherwise($"rcse_curr_terminal_sw_id"))
        .select("date_id", "natco_code", "msisdn", "rcse_tc_status_id", "rcse_curr_client_id", "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id")

    result.show(false)
    println(result.count())

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


  }
}
