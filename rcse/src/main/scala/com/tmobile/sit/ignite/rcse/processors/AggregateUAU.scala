package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{CommonStructures, Terminal}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

class AggregateUAU(time_key: Date, settings: Settings)(implicit sparkSession: SparkSession) extends Processor {

  import sparkSession.implicits._

  override def processData(): Unit = {
    val aggregKeys = Map(
      "key0" -> Seq("rcse_terminal_vendor_ldesc", "rcse_terminal_id", "rcse_client_vendor_ldesc", "rcse_client_id", "rcse_terminal_sw_id", "natco_code"),
      "key1" -> Seq("rcse_terminal_vendor_ldesc", "rcse_client_vendor_ldesc", "rcse_client_id", "rcse_terminal_sw_id", "natco_code"),
      "key2" -> Seq("rcse_terminal_vendor_ldesc", "rcse_terminal_id", "rcse_client_vendor_ldesc", "rcse_terminal_sw_id", "natco_code"),
      "key3" -> Seq("rcse_terminal_vendor_ldesc", "rcse_terminal_id", "rcse_client_vendor_ldesc", "rcse_client_id", "natco_code"),
      "key4" -> Seq("rcse_terminal_vendor_ldesc", "rcse_terminal_id", "rcse_client_vendor_ldesc", "natco_code"),
      "key5" -> Seq("rcse_terminal_vendor_ldesc", "rcse_terminal_id", "rcse_terminal_sw_id", "natco_code"),
      "key6" -> Seq("rcse_terminal_vendor_ldesc", "rcse_client_vendor_ldesc", "rcse_client_id", "natco_code"),
      "key7" -> Seq("rcse_terminal_vendor_ldesc", "rcse_client_vendor_ldesc", "rcse_terminal_sw_id", "natco_code"),
      "key8" -> Seq("rcse_client_vendor_ldesc", "rcse_client_id", "rcse_terminal_sw_id", "natco_code"),
      "key9" -> Seq("rcse_terminal_vendor_ldesc", "rcse_terminal_id", "natco_code"),
      "key10" -> Seq("rcse_terminal_vendor_ldesc", "rcse_client_vendor_ldesc", "natco_code"),
      "key11" -> Seq("rcse_terminal_vendor_ldesc", "rcse_terminal_sw_id", "natco_code"),
      "key12" -> Seq("rcse_client_vendor_ldesc", "rcse_client_id", "natco_code"),
      "key13" -> Seq("rcse_client_vendor_ldesc", "rcse_terminal_sw_id", "natco_code"),
      "key14" -> Seq("rcse_terminal_vendor_ldesc", "natco_code"),
      "key15" -> Seq("rcse_client_vendor_ldesc", "natco_code"),
      "key16" -> Seq("rcse_terminal_sw_id", "natco_code"),
      "key17" -> Seq("natco_code")
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


    val activeUsersData = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_active_user.TMD.20200607.csv",
      header = false,
      delimiter = "|",
      schema = Some(activeUsersSchema)
    )
      .read()
      .drop("entry_id", "load_date")

    val client = CSVReader(
      path = settings.clientPath,
      header = false,
      schema = Some(CommonStructures.clientSchema),
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      delimiter = "|"
    )
      .read()
      .select("rcse_client_id", "rcse_client_vendor_ldesc")

    val terminal = CSVReader(path = settings.terminalPath,
      header = false,
      schema = Some(Terminal.terminal_d_struct),
      delimiter = "|")
      .read()
      .select("rcse_terminal_id", "rcse_terminal_vendor_ldesc")


    val activeUsersPreprocessed = activeUsersData
      .sort("msisdn")
      .groupBy("msisdn")
      .agg(
        first("date_id").alias("date_id"),
        first("natco_code").alias("natco_code"),
        first("rcse_tc_status_id").alias("rcse_tc_status_id"),
        first("rcse_curr_client_id").alias("rcse_curr_client_id"),
        first("rcse_curr_terminal_id").alias("rcse_curr_terminal_id"),
        first("rcse_curr_terminal_sw_id").alias("rcse_curr_terminal_sw_id")
      )
      .withColumnRenamed("rcse_curr_client_id", "rcse_client_id")
      .withColumnRenamed("rcse_curr_terminal_id", "rcse_terminal_id")
      .withColumnRenamed("rcse_curr_terminal_sw_id", "rcse_terminal_sw_id")
      .join(client, Seq("rcse_client_id"), "left_outer")
      .join(terminal, Seq("rcse_terminal_id"), "left_outer")
      .cache()

    println(
      activeUsersPreprocessed.count()
    )

    val result = (for (i <- aggregKeys.keySet) yield {
      val aggreg = activeUsersPreprocessed
        .groupBy(aggregKeys(i).map(col(_)): _*)
        .agg(
          count("*").alias("unique_users")
        )
      aggregKeys("key0")
        .foldLeft(aggreg)((df, name) => if (aggregKeys(i).filter(_ == name).isEmpty) df.withColumn(name, lit("##")) else df)
        .withColumn("date_id", lit(time_key))
    })
      .reduce(_.union(_))
      .withColumn("rcse_client_key_code", concat_ws("_", $"rcse_client_vendor_ldesc", $"rcse_client_id"))
      .withColumn("rcse_terminal_key_code", concat_ws("_", $"rcse_client_vendor_ldesc", $"rcse_terminal_id"))
      .withColumnRenamed("rcse_terminal_sw_id", "rcse_terminal_sw_key_code")
      .select(
        $"date_id".as("time_key_code"),
        $"natco_code".as("natco_key_code"),
        $"rcse_client_key_code",
        $"rcse_terminal_key_code",
        $"rcse_terminal_sw_key_code",
        $"unique_users"
      )

    result
      .coalesce(1)
      .write
      .option("delimiter", "|")
      .option("header", "false")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_x_rcse_uau_d.TMD.20200607.csv");


  }

}
