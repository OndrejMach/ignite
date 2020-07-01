package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.events.EventsInputData
import com.tmobile.sit.ignite.rcse.structures.{Conf, InitConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class InitConfAggregatesProcessor(processingDate: Date, settings: Settings)(implicit sparkSession: SparkSession) extends Processor {

  import sparkSession.implicits._

  override def processData(): Unit = {
    val inputData: EventsInputData = new EventsInputData(settings = settings)

    val confData = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      schema = Some(Conf.confFileSchema),
      header = false,
      delimiter = "|"
    ).read()

    val initData = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_x_rcse_init_conf.TMD.csv",
      header = false,
      delimiter = "|",
      schema = Some(InitConf.initConfSchema)
    )
      .read()


    val tacPreprocessed = inputData.tac
      .filter($"valid_to" >= MAX_DATE && $"id".isNotNull)
      .terminalSimpleLookup(inputData.terminal)
      .withColumn("rcse_terminal_id_tac", $"rcse_terminal_id_tac")
      .withColumnRenamed("rcse_terminal_id_terminal", "rcse_terminal_id_term")
      .drop("rcse_terminal_id_desc")

    val confPreprocessed = confData
      .filter($"date_id" === lit(processingDate))
      .withColumn("accepted", when($"rcse_tc_status_id" === lit(1) || $"rcse_tc_status_id" === lit(1), lit(1)).otherwise(lit(0)))
      .withColumn("denied", when($"rcse_tc_status_id" === lit(2), lit(1)).otherwise(lit(0)))
      .groupBy("rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        first("date_id").alias("date_id"),
        first("natco_code").alias("natco_code"),
        sum("accepted").alias("rcse_num_tc_acc"),
        sum("denied").alias("rcse_num_tc_den")
      )
      .select(InitConf.stageColumns.head, InitConf.stageColumns.tail: _*)

    //potentially common
    val initDataPreprocessed = initData
      .filter($"date_id" =!= lit(processingDate))
      .select(InitConf.stageColumns.head, InitConf.stageColumns.tail: _*)
      .union(confPreprocessed)


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

    val duplicates = changedAll
      .join(
        changed.select($"id".as("id_agg")),
        $"id" === $"id_agg",
        "left")
      .filter($"id_agg".isNull)
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)

    val join1Work = initDataPreprocessed
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

    val join1 = join1Work
      .filter($"l" === lit(1) && $"rcse_old_terminal_id_right".isNotNull)
      .withColumn("rcse_old_terminal_id", $"rcse_old_terminal_id_right")
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)

    val unmatched = join1Work
      .filter($"l".isNull)
      .drop("l", "r")
      .select(InitConf.workColumns.map(i => col(i + "_right").alias(i)): _*)

    val teeABSort = duplicates
      .union(join1)

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
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)


    //here it differs
    val join3 = join2
      .withColumn("l", lit(1))
      .join(
        teeABSort
        .toDF(teeABSort.columns.map(_+"_right") :_*)
          .withColumn("r", lit(1)),
        $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_init_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "left"
      )
      .withColumn("rcse_num_tc_acc", when($"r".isNotNull, $"rcse_num_tc_acc" + $"rcse_num_tc_acc_right"))
      .withColumn("rcse_num_tc_den", when($"r".isNotNull, $"rcse_num_tc_den" + $"rcse_num_tc_den_right"))
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)

    val result = join3
      .withColumn("l", lit(1))
      .join(
        teeABSort
          .toDF(teeABSort.columns.map(_+"_right") :_*)
          .withColumn("r", lit(1)),
        $"date_id" === $"date_id_right" &&
          $"rcse_init_client_id" === $"rcse_init_client_id_right" &&
          $"rcse_init_terminal_id" === $"rcse_old_terminal_id_right" &&
          $"rcse_init_terminal_sw_id" === $"rcse_init_terminal_sw_id_right",
        "left"
      )
      .filter($"r".isNull)
      .select(InitConf.workColumns.head, InitConf.workColumns.tail: _*)

    logger.info(s"result count ${result.count()}")
  }
}
