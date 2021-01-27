package com.tmobile.sit.ignite.rcse.processors.inituseragregates

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.inputs.{InitUserInputs, LookupsData, LookupsDataReader}
import com.tmobile.sit.ignite.rcse.processors.udfs.UDFs
import com.tmobile.sit.ignite.rcse.structures.InitUsers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, count, datediff, explode, first, lit, max, monotonically_increasing_id, udf, when, asc, last}
import org.apache.spark.sql.types.{DateType, IntegerType}
import com.tmobile.sit.ignite.rcse.processors.Lookups

/**
 * The core og the init User aggregates logic. Gets inputs and performs all the calculations for the resulting data. This is a bit tricky
 * because a UDF is used for calculation of number of users for each group by key class.
 * @param inputData - inputs
 * @param lookups - terminal, tac, client
 * @param maxDate - validity limit for the actually valid rows.
 * @param processingDate - date for which data is calculated
 * @param sparkSession
 */

class InitUserAggregatesProcessor(inputData: InitUserInputs, lookups: LookupsData, maxDate: Date, processingDate: Date)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._


  private val tacPreprocessed = {
    logger.info("Preparing TAC")
    lookups.tac
      .filter($"valid_to" >= maxDate && $"id".isNotNull)
      .terminalSimpleLookup(lookups.terminal)
      .withColumn("rcse_terminal_id_tac", $"rcse_terminal_id_tac")
      .withColumnRenamed("rcse_terminal_id_terminal", "rcse_terminal_id_term")
      .drop("rcse_terminal_id_desc")
  }

  private val userPreprocessed = {
    logger.info("Preparing init user data")
    val filtered = inputData.confData
      .filter($"date_id" >= lit(processingDate) && ($"rcse_tc_status_id" === lit(0) || $"rcse_tc_status_id" === lit(1)))

    logger.debug(s"filtered count ${filtered.count()}")

     val ret= filtered
      .groupBy("date_id", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        count("*").alias("rcse_reg_users_new"),
        first("natco_code").alias("natco_code")
      )
      .withColumn("rcse_reg_users_all", lit(0))
      .select(InitUsers.stageColumns.head, InitUsers.stageColumns.tail: _*)

    logger.debug(s"conf count: ${ret.count()}")
    ret
  }


  //potentially common

  private val initDate1 = {
    logger.info("Calculating initDate1")
    val maxDateId = inputData.initData.select(max("date_id")).collect()(0).getDate(0)

    logger.info(s"maxDateId: ${maxDateId}")

    val getDates = udf(UDFs.dateUDF)

    val processingDateMinus1 = Date.valueOf(processingDate.toLocalDate.minusDays(1))
    val refDate = Date.valueOf(LocalDate.of(1900, 1, 1))

    val ret = inputData.initData
      .drop("load_date", "entry_id")
      .filter($"date_id" === lit(Date.valueOf(processingDate.toLocalDate.minusDays(1))))
      .union(userPreprocessed)
      //.sort("date_id")
      .withColumn("date_id_upper_bound", when(lit(processingDate) <= lit(maxDateId), lit(maxDateId)).otherwise(lit(processingDate)))
      .withColumn("cnt_users_all", when($"date_id" === lit(processingDateMinus1), $"rcse_reg_users_all").otherwise(0))
      .groupBy("rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        collect_list(when($"date_id" =!= lit(processingDateMinus1), datediff($"date_id", lit(refDate)).cast(IntegerType))).alias("date_queue"),
        collect_list(when($"date_id" =!= lit(processingDateMinus1), $"rcse_reg_users_new".cast(IntegerType))).alias("user_queue"),
        max($"cnt_users_all").alias("cnt_users_all"),
        max("date_id_upper_bound").alias("date_id_upper_bound"),
        first("natco_code").alias("natco_code")
      )
      .withColumn("dateCounts", getDates(lit(processingDate).cast(DateType), $"date_queue", $"user_queue", $"date_id_upper_bound", $"cnt_users_all"))
      .withColumn("date_metrics", explode($"dateCounts"))
      .withColumn("date_id", $"date_metrics".getItem("date_id"))
      .withColumn("rcse_reg_users_new", $"date_metrics".getItem("rcse_reg_users_new"))
      .withColumn("rcse_reg_users_all", $"date_metrics".getItem("rcse_reg_users_all"))
      .select("date_id", "natco_code", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id", "rcse_reg_users_new", "rcse_reg_users_all")

    logger.debug(s"INIT_DATE1 count: ${ret.count()}")
    ret
  }

  def getData = {
    logger.info("Getting result")

    val initUpdated = inputData.initData
      .drop("load_date", "entry_id")
      .filter($"date_id" < lit(processingDate))
      .union(initDate1)

    logger.debug(s"initUpdated count ${initUpdated.count()}")

    val changedAll = initUpdated
      .join(tacPreprocessed.select("rcse_terminal_id_tac", "rcse_terminal_id_term").withColumn("e", lit(1)),
        $"rcse_terminal_id_tac" === $"rcse_init_terminal_id", "left_outer"
      )
      .filter($"e" === lit(1))
      .filter($"rcse_terminal_id_term".isNotNull && ($"rcse_terminal_id_term" =!= $"rcse_init_terminal_id"))
      .withColumn("rcse_old_terminal_id", $"rcse_init_terminal_id")
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)
      .withColumn("id", monotonically_increasing_id())

    logger.debug(s"changedAll count ${changedAll.count()}")

    val changed = changedAll
     // .sort(asc("rcse_old_terminal_id"), asc("id"))
      .groupBy("date_id", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        first("natco_code").alias("natco_code"),
        max("rcse_old_terminal_id").alias("rcse_old_terminal_id"),
        max("rcse_reg_users_new").alias("rcse_reg_users_new"),
        max("rcse_reg_users_all").alias("rcse_reg_users_all"),
        max("id").alias("id")
      )

    logger.debug(s"changed count ${changed.count()}")

    val duplicates = changedAll
      .join(
        changed.select($"id".as("id_agg")),
        $"id" === $"id_agg",
        "left")
      .filter($"id_agg".isNull)
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)

    logger.debug(s"duplicates count ${duplicates.count()}")

    val join1Work = initUpdated
      .withColumn("l", lit(1))
      .join(
        changed
          .drop("id")
          .toDF(changed.columns.filter(_ != "id").map(_ + "_right"): _*)
          .withColumn("r", lit(1)),
        $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_init_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "right")

    logger.debug(s"join1Work count ${join1Work.count()}")

    val join1 = join1Work
      .filter($"l" === lit(1) && $"rcse_old_terminal_id_right".isNotNull)
      .withColumn("rcse_old_terminal_id", $"rcse_old_terminal_id_right")
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)

    logger.debug(s"join1 count ${join1.count()}")

    val unmatched = join1Work
      .filter($"l".isNull)
      .drop("l", "r")
      .select(InitUsers.workColumns.map(i => col(i + "_right").alias(i)): _*)
      .withColumn("rcse_init_terminal_id", $"rcse_old_terminal_id")

    logger.debug(s"unmatched count ${unmatched.count()}")

    val teeABSort = duplicates
      .union(join1)

    logger.debug(s"teeABSort count ${teeABSort.count()}")

    val join2 = initUpdated
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
      .select(InitUsers.stageColumns.head, InitUsers.stageColumns.tail: _*)

    logger.debug(s"join2 count ${join2.count()}")

    val join3 = join2
      .withColumn("l", lit(1))
      .join(
        teeABSort
          .toDF(teeABSort.columns.map(_ + "_right"): _*)
          .withColumn("r", lit(1)),
        $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_init_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "left"
      )
      .withColumn("rcse_reg_users_new", when($"r".isNotNull, $"rcse_reg_users_new" + $"rcse_reg_users_new_right").otherwise($"rcse_reg_users_new"))
      .withColumn("rcse_reg_users_all", when($"r".isNotNull, $"rcse_reg_users_all" + $"rcse_reg_users_all_right").otherwise($"rcse_reg_users_all"))
      .select(InitUsers.stageColumns.head, InitUsers.stageColumns.tail: _*)

    logger.debug(s"join3 count ${join3.count()}")

    val ret = join3
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
      .select(InitUsers.stageColumns.head, InitUsers.stageColumns.tail: _*)
    logger.debug(s"ret count ${ret.count()}")
    ret
  }
}
