package com.tmobile.sit.ignite.rcse.processors.initconfaggregates

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.inputs.{InitConfInputs, LookupsData}
import com.tmobile.sit.ignite.rcse.structures.InitConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first, lit, monotonically_increasing_id, sum, when}

import com.tmobile.sit.ignite.rcse.processors.Lookups

class InitConfAggregatesProcessor(lookups: LookupsData, inputData: InitConfInputs, maxDate: Date, processingDate: Date)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  private lazy val tacPreprocessed = {
    logger.info("Preparing TAC data")
    lookups.tac
      .filter($"valid_to" >= maxDate && $"id".isNotNull)
      .terminalSimpleLookup(lookups.terminal)
      .withColumn("rcse_terminal_id_tac", $"rcse_terminal_id_tac")
      .withColumnRenamed("rcse_terminal_id_terminal", "rcse_terminal_id_term")
      .drop("rcse_terminal_id_desc")
  }

  private lazy val confPreprocessed = {
    logger.info("Preparing Conf data")
    inputData.confData
      .filter($"date_id" === lit(processingDate))
      .withColumn("accepted", when($"rcse_tc_status_id" === lit(0) || $"rcse_tc_status_id" === lit(1), lit(1)).otherwise(lit(0)))
      .withColumn("denied", when($"rcse_tc_status_id" === lit(2), lit(1)).otherwise(lit(0)))
      .groupBy("rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        first("date_id").alias("date_id"),
        first("natco_code").alias("natco_code"),
        sum("accepted").alias("rcse_num_tc_acc"),
        sum("denied").alias("rcse_num_tc_den")
      )
      .select(InitConf.stageColumns.head, InitConf.stageColumns.tail: _*)
  }

  //potentially common
  private lazy val initDataPreprocessed = {
    logger.info("preparing init Data")
    inputData.initData
      .filter($"date_id" =!= lit(processingDate))
      .select(InitConf.stageColumns.head, InitConf.stageColumns.tail: _*)
      .union(confPreprocessed)
  }

  val getData = {
    logger.info("Getting changed data")
    val changedAll = initDataPreprocessed
      .join(tacPreprocessed.select("rcse_terminal_id_tac", "rcse_terminal_id_term").withColumn("e", lit(1)),
        $"rcse_terminal_id_tac" === $"rcse_init_terminal_id", "left_outer"
      )
      .filter($"e" === lit(1))
      .filter($"rcse_terminal_id_term".isNotNull && ($"rcse_terminal_id_term" =!= $"rcse_init_terminal_id"))
      .withColumn("rcse_old_terminal_id", $"rcse_init_terminal_id")
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)
      .withColumn("id", monotonically_increasing_id())

    val changed = changedAll
      .sort("date_id", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .groupBy("date_id", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        first("natco_code").alias("natco_code"),
        first("rcse_old_terminal_id").alias("rcse_old_terminal_id"),
        first("rcse_num_tc_acc").alias("rcse_num_tc_acc"),
        first("rcse_num_tc_den").alias("rcse_num_tc_den"),
        first("id").alias("id")
      )

    logger.info("Getting duplicates")
    val duplicates = changedAll
      .join(
        changed.select($"id".as("id_agg")),
        $"id" === $"id_agg",
        "left")
      .filter($"id_agg".isNull)
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)

    logger.info("Joining with init data")
    val join1Work = initDataPreprocessed
      .withColumn("l", lit(1))
      .join(
        changed
          .drop("id")
          .toDF(changed
            .columns
            .filter(_ != "id")
            .map(_ + "_right"): _*)
          .withColumn("r", lit(1)),
        $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_init_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "right")

    val join1 = join1Work
      .filter($"l" === lit(1) && $"rcse_old_terminal_id_right".isNotNull)
      .withColumn("rcse_old_terminal_id", $"rcse_old_terminal_id_right")
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)

    logger.info("Filteing unmatched")
    val unmatched = join1Work
      .filter($"l".isNull)
      .drop("l", "r")
      .select(InitConf.workColumns.map(i => col(i + "_right").alias(i)): _*)

    val teeABSort = duplicates
      .union(join1)

    logger.info("Merging all together")
    val join2 = initDataPreprocessed
      .withColumn("l", lit(1))
      .join(unmatched

        .toDF(unmatched.columns.map(_ + "_right"): _*)
        .withColumn("r", lit(1))
        , $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_old_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "left"
      )
      .withColumn("rcse_init_terminal_id", when($"r".isNotNull, $"rcse_init_terminal_id_right").otherwise($"rcse_init_terminal_id"))
      .withColumnRenamed("rcse_old_terminal_id_right", "rcse_old_terminal_id")
      .select(InitConf.stageColumns.head, InitConf.stageColumns.tail: _*)



    val join3 = join2
      .withColumn("l", lit(1))
      .join(
        teeABSort
          .toDF(teeABSort
            .columns
            .map(_ + "_right"): _*)
          .withColumn("r", lit(1)),
        $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_init_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "left"
      )
      .withColumn("rcse_num_tc_acc", when($"r".isNotNull, $"rcse_num_tc_acc" + $"rcse_num_tc_acc_right").otherwise($"rcse_num_tc_acc"))
      .withColumn("rcse_num_tc_den", when($"r".isNotNull, $"rcse_num_tc_den" + $"rcse_num_tc_den_right").otherwise($"rcse_num_tc_den"))
      .select(InitConf.stageColumns.head, InitConf.stageColumns.tail: _*)

    join3
      .withColumn("l", lit(1))
      .join(
        teeABSort
          .toDF(teeABSort.columns.map(_ + "_right"): _*)
          .withColumn("r", lit(1)),
        $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_old_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "left"
      )
      .filter($"r".isNull)
      .select(InitConf.stageColumns.head, InitConf.stageColumns.tail: _*)
  }
}
