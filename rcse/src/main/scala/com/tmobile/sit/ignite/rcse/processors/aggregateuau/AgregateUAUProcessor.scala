package com.tmobile.sit.ignite.rcse.processors.aggregateuau

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.inputs.{AgregateUAUInputs, LookupsData, LookupsDataReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, count, first, lit}
import org.apache.spark.storage.StorageLevel

/**
 * Aggregates calculated from the active user file. it creates aggregates for 17 different aggregation keys and stores them in the output data.
 * In case the key is shorter than the biggest one column is filled with '##
 * @param inputs - input data
 * @param lookups - terminal and client data for lookup
 * @param time_key - processing date in fact
 * @param sparkSession
 */

class AgregateUAUProcessor(inputs: AgregateUAUInputs, lookups: LookupsData, time_key:Date)(implicit sparkSession: SparkSession) extends Logger {
  import sparkSession.implicits._

  private val aggregKeys = Map(
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



  private val activeUsersPreprocessed = {
    logger.info("GPreprocessing active users")

    val clientCols = lookups.client.columns.filter(_ !="rcse_client_id").map(i => first(i).alias(i))

    val client = lookups.client
        .groupBy("rcse_client_id")
        .agg(
          clientCols.head,
          clientCols.tail :_*
        )
    val terminalCols = lookups.terminal.columns.filter(_!="rcse_terminal_id").map(i=>first(i).alias(i) )

    val terminal = lookups.terminal
        .groupBy("rcse_terminal_id")
        .agg(
          terminalCols.head,
          terminalCols.tail :_*
        )

    inputs.activeUsersData
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
      .persist(StorageLevel.MEMORY_ONLY)
  }


  val result = {
    logger.info("Calculating final result")
    (for (i <- aggregKeys.keySet) yield {
      val aggreg = activeUsersPreprocessed
        .groupBy(aggregKeys(i).map(col(_)): _*)
        .agg(
          count("*").alias("unique_users")
        )
      aggregKeys("key0")
        .foldLeft(aggreg)((df, name) => if (aggregKeys(i).filter(_ == name).isEmpty) df.withColumn(name, lit("##")) else df)
        .withColumn("date_id", lit(time_key))
        .withColumn("rcse_client_key_code", concat_ws("_", $"rcse_client_vendor_ldesc", $"rcse_client_id"))
        .withColumn("rcse_terminal_key_code", concat_ws("_", $"rcse_terminal_vendor_ldesc", $"rcse_terminal_id"))
        .withColumnRenamed("rcse_terminal_sw_id", "rcse_terminal_sw_key_code")
        .select(
          $"date_id".as("time_key_code"),
          $"natco_code".as("natco_key_code"),
          $"rcse_client_key_code",
          $"rcse_terminal_key_code",
          $"rcse_terminal_sw_key_code",
          $"unique_users"
        )
    })
      .reduce(_.union(_))
  }

}
