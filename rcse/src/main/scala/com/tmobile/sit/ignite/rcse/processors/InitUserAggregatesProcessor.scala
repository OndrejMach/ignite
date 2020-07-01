package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.events.EventsInputData
import com.tmobile.sit.ignite.rcse.structures.{Conf, InitConf, InitUsers}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import com.tmobile.sit.ignite.rcse.processors.udfs.UDFs
import org.apache.spark.sql.types.{DateType, IntegerType}

case class DatesCount(date_id: Date, rcse_reg_users_new: Int, rcse_reg_users_all: Int)


class InitUserAggregatesProcessor(processingDate: Date, settings: Settings)(implicit sparkSession: SparkSession) extends Processor {

  import sparkSession.implicits._

  val processingDateMinus1 = Date.valueOf(processingDate.toLocalDate.minusDays(1))
  val refDate = Date.valueOf(LocalDate.of(1900, 1, 1))

  override def processData(): Unit = {
    val inputData: EventsInputData = new EventsInputData(settings = settings)

    val confData = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      schema = Some(Conf.confFileSchema),
      header = false,
      delimiter = "|"
    ).read()

    val initData = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_x_rcse_init_user.TMD.csv",
      header = false,
      delimiter = "|",
      schema = Some(InitUsers.initUsersSchema)
    )
      .read()


    val tacPreprocessed = inputData.tac
      .filter($"valid_to" >= MAX_DATE && $"id".isNotNull)
      .terminalSimpleLookup(inputData.terminal)
      .withColumn("rcse_terminal_id_tac", $"rcse_terminal_id_tac")
      .withColumnRenamed("rcse_terminal_id_terminal", "rcse_terminal_id_term")
      .drop("rcse_terminal_id_desc")

    val userPreprocessed = confData
      .filter($"date_id" === lit(processingDate) && ($"rcse_tc_status_id" === lit(0) || $"rcse_tc_status_id" === lit(1)))
      .groupBy("date_id", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        count("*").alias("rcse_reg_users_new"),
        first("natco_code").alias("natco_code")
      )
      .withColumn("rcse_reg_users_all", lit(0))
      .select(InitUsers.stageColumns.head, InitUsers.stageColumns.tail: _*)

    /*static const date ref_date = date(1900,1,1);
static const date max_date_id = date(std::getenv("MAX_DATE_ID"),"%Y-%m-%d");
static const date odate = date(std::getenv("ODATE"),"%Y%m%d");
static const date odate_minus1 = date(std::getenv("ODATE_MINUS1"),"%Y%m%d");
static date date_id;
static date date_id_upper_bound;
static int cnt_users_all;
static std::deque<int> date_queue;
static std::deque<int> user_queue;

INITIALIZE:
cnt_users_all = 0;
date_queue.clear();
user_queue.clear();
date_id = odate;
if (odate <= max_date_id)
  date_id_upper_bound = max_date_id;
else
  date_id_upper_bound = odate;

COMPUTE:
if (*in->date_id == odate_minus1)
  cnt_users_all = *in->rcse_reg_users_all;
else
{
  date_queue.push_back(*in->date_id - ref_date); // days since 1900-01-01
  user_queue.push_back(*in->rcse_reg_users_new);
}

FINALIZE:
$SET_RUN_ID
$SET_LOAD_DATE

out->natco_code 		= in->natco_code;
out->rcse_init_client_id 	= in->rcse_init_client_id;
out->rcse_init_terminal_id 	= in->rcse_init_terminal_id;
out->rcse_init_terminal_sw_id 	= in->rcse_init_terminal_sw_id;
*out->entry_id 			= run_id;
*out->load_date 		= load_date;

while (date_id <= date_id_upper_bound)
{
  *out->date_id 		= date_id;
  if (!date_queue.empty() && date_queue.front() == date_id - ref_date)
  {
    *out->rcse_reg_users_new	= user_queue.front();
    cnt_users_all 	       += user_queue.front();
    date_queue.pop_front();
    user_queue.pop_front();
  }
  else
    *out->rcse_reg_users_new	= 0;

  *out->rcse_reg_users_all 	= cnt_users_all;
  add_record();

  date_id++;
}*/


    val getDates = udf(UDFs.dateUDF)


    //potentially common
    val maxDateId = initData.select(max("date_id")).collect()(0).getDate(0)


    val initDate1 = initData
      .drop("load_date", "entry_id")
      .filter($"date_id" === lit(Date.valueOf(processingDate.toLocalDate.minusDays(1))))
      .union(userPreprocessed)
      .sort("rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id", "date_id")
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
      //(date_id: Date,date_queue: List[Int], user_queue: List[Int], dateUpperBound: Date, cnt_users_all: Int )
      .withColumn("dateCounts", getDates(lit(processingDate).cast(DateType), $"date_queue", $"user_queue", $"date_id_upper_bound", $"cnt_users_all"))
      .withColumn("date_metrics", explode($"dateCounts"))
      .withColumn("date_id", $"date_metrics".getItem("date_id"))
      .withColumn("rcse_reg_users_new", $"date_metrics".getItem("rcse_reg_users_new"))
      .withColumn("rcse_reg_users_all", $"date_metrics".getItem("rcse_reg_users_all"))
      .select("date_id", "natco_code", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id", "rcse_reg_users_new", "rcse_reg_users_all" )


    val initUpdated = initData
      .drop("load_date", "entry_id")
      .filter($"date_id" < lit(processingDate))
      .union(initDate1)

    val changedAll = initUpdated
      .join(tacPreprocessed.select("rcse_terminal_id_tac", "rcse_terminal_id_term").withColumn("e", lit(1)),
        $"rcse_terminal_id_tac" === $"rcse_init_terminal_id", "left_outer"
      )
      .filter($"e" === lit(1))
      .filter($"rcse_terminal_id_term".isNotNull && ($"rcse_terminal_id_term" =!= $"rcse_init_terminal_id"))
      .withColumn("rcse_old_terminal_id", $"rcse_init_terminal_id")
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)
      .withColumn("id", monotonically_increasing_id())

    val changed = changedAll
      .sort("date_id", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .groupBy("date_id", "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id")
      .agg(
        first("natco_code").alias("natco_code"),
        first("rcse_old_terminal_id").alias("rcse_old_terminal_id"),
        first("rcse_reg_users_new").alias("rcse_reg_users_new"),
        first("rcse_reg_users_all").alias("rcse_reg_users_all"),
        first("id").alias("id")
      )

    val duplicates = changedAll
      .join(
        changed.select($"id".as("id_agg")),
        $"id" === $"id_agg",
        "left")
      .filter($"id_agg".isNull)
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)

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

    val join1 = join1Work
      .filter($"l" === lit(1) && $"rcse_old_terminal_id_right".isNotNull)
      .withColumn("rcse_old_terminal_id", $"rcse_old_terminal_id_right")
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)

    val unmatched = join1Work
      .filter($"l".isNull)
      .drop("l", "r")
      .select(InitUsers.workColumns.map(i => col(i + "_right").alias(i)): _*)

    val teeABSort = duplicates
      .union(join1)

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
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)

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
      .withColumn("rcse_reg_users_new", when($"r".isNotNull, $"rcse_reg_users_new" + $"rcse_reg_users_new_right"))
      .withColumn("rcse_reg_users_all", when($"r".isNotNull, $"rcse_reg_users_all" + $"rcse_reg_users_all_right"))
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)

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
      .select(InitUsers.workColumns.head, InitUsers.workColumns.tail: _*)

    logger.info(s"result count ${result.count()}")

  }
}
