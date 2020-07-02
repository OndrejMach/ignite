package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.{Conf, Terminal}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first, length, lit, trim, when}

class ConfToStage(settings: Settings, max_Date: Date, processing_date: Date)(implicit sparkSession: SparkSession) extends Processor {

  override def processData(): Unit = {
    import sparkSession.implicits._

    val outColumns = Seq("date_id", "natco_code",
      "msisdn", "rcse_tc_status_id",
      "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id",
      "rcse_curr_client_id", "rcse_curr_terminal_id",
      "rcse_curr_terminal_sw_id", "modification_date")


    val tacTerminal = CSVReader(
      path = settings.tacPath,
      header = false,
      schema = Some(Terminal.tac_struct),
      delimiter = "|"
    )
      .read()
      .filter($"valid_to" >= lit(MAX_DATE))
      .withColumn("terminal_id", $"id")

    val terminal = CSVReader(path = settings.terminalPath,
      header = false,
      schema = Some(Terminal.terminal_d_struct),
      delimiter = "|")
      .read()

    val events = CSVReader(path = settings.terminalPath,
      header = false,
      schema = Some(Terminal.terminalSchema),
      delimiter = "|")
      .read()

    val confData = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_conf.TMD.csv",
      header = false,
      schema = Some(Conf.confFileSchema),
      delimiter = "|"
    ).read()


    val preprocessedTac = tacTerminal
      .filter($"valid_to" >= lit(max_Date) && $"id".isNotNull)
      .join(terminal.select("tac_code","rcse_terminal_id"), Seq("tac_code"), "left_outer")
      .withColumn("rcse_terminal_id_tac", $"rcse_terminal_id")
      .drop("rcse_terminal_id")
      .join(terminal.select($"rcse_terminal_id", $"terminal_id".as("terminal_id_terminal")),$"terminal_id_terminal" === $"id", "left_outer")
      .withColumn("rcse_terminal_id_term", $"rcse_terminal_id")
      .drop("rcse_terminal_id", "terminal_id_terminal")
      .select($"tac_code", $"id".as("terminal_id"), $"rcse_terminal_id_tac", $"rcse_terminal_id_term".as("rcse_terminal_id_term"))


    val preprocessedEvents = events
      .filter($"rcse_subscribed_status_id" === lit(1) && $"rcse_active_status_id" === lit(1))
      .join(preprocessedTac, $"rcse_terminal_id_tac" === $"rcse_terminal_id", "left_outer")
      .withColumn("rcse_curr_terminal_id", when($"rcse_terminal_id_term".isNotNull, $"rcse_terminal_id_term").otherwise($"rcse_terminal_id"))
      .withColumn("rcse_curr_terminal_sw_id", $"rcse_terminal_sw_id")
      .withColumn("modification_date", $"date_id")
      .withColumn("rcse_init_client_id", $"rcse_client_id")
      .withColumn("rcse_init_terminal_id", $"rcse_terminal_id")
      .withColumn("rcse_init_terminal_sw_id", $"rcse_terminal_sw_id")
      .withColumn("rcse_curr_client_id", $"rcse_client_id")
      .select(
        "date_id", "natco_code",
        "msisdn", "rcse_tc_status_id",
        "rcse_init_client_id", "rcse_init_terminal_id",
        "rcse_init_terminal_sw_id", "rcse_curr_client_id",
        "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id",
        "modification_date"
      )

    val conf2 = confData
      .join(
        preprocessedTac.withColumn("e", lit(1)).select("rcse_terminal_id_tac", "rcse_terminal_id_term", "e"),
        $"rcse_curr_terminal_id" === $"rcse_terminal_id_tac", "left_outer")
      .withColumn("term", $"rcse_terminal_id_term")
      .filter($"term".isNotNull && $"e" === lit(1) &&
        (($"rcse_curr_terminal_id".isNotNull && $"term" =!= $"rcse_curr_terminal_id") ||
          $"rcse_curr_terminal_id".isNull))
      .withColumn("rcse_curr_terminal_id", $"term")
      .withColumn("modification_date", lit(processing_date))
      .sort("msisdn")
      .groupBy("msisdn")
      .agg(
        first(outColumns.head).alias(outColumns.head),
        outColumns.tail.filter(_ != "msisdn").map(i => first(i).alias(i)) :_*
      )
      .select(
      outColumns.head, outColumns.tail :_*
    )


    val confColumns = confData.columns.map(_+"_conf")
    val joinedEventsConfData = preprocessedEvents
      .join(
        confData
          .toDF(confColumns :_*)
          .withColumn("e", lit(1)),
        $"msisdn" === $"msisdn_conf",
        "left"
      )

    val umatched = joinedEventsConfData
      .filter($"e".isNull)
      .select(
        outColumns.map(i => col(i+"_conf").as(i)) :_*
      )
      .filter($"msisdn".isNotNull)
      .persist()

    logger.info(s"Unmatched count: ${umatched.count()}")


    val joined = joinedEventsConfData
      .filter($"e".isNotNull)
      .withColumn("modification_date", $"date_id")
      .withColumn("date_id", $"date_id_conf")
      .withColumn("rcse_tc_status_id", $"rcse_tc_status_id_conf")
      .withColumn("rcse_init_client_id", $"rcse_init_client_id_conf")
      .withColumn("rcse_init_terminal_id", $"rcse_init_terminal_id_conf")
      .withColumn("rcse_init_terminal_sw_id", $"rcse_init_terminal_sw_id_conf")
      .select(
        outColumns.head, outColumns.tail :_*
      )


    val tmpUpdate = joined
      .union(conf2)


    val allCols = tmpUpdate.columns.map(_+"_conf_update")
    val updJoin = confData
      .join(tmpUpdate.toDF(allCols :_*), $"msisdn" === $"msisdn_conf_update", "left")
      .filter($"msisdn".isNotNull)

    .withColumn("date_id", when($"date_id_conf_update".isNotNull, $"date_id_conf_update").otherwise($"date_id"))
      .withColumn("msisdn", when($"msisdn_conf_update".isNotNull, $"msisdn_conf_update").otherwise($"msisdn"))
      .withColumn("rcse_tc_status_id",
        when($"rcse_tc_status_id_conf_update".isNotNull, $"rcse_tc_status_id_conf_update")
          .otherwise($"rcse_tc_status_id"))
      .withColumn("rcse_init_client_id",
        when($"rcse_init_client_id_conf_update".isNotNull, $"rcse_init_client_id_conf_update")
          .otherwise($"rcse_init_client_id"))
      .withColumn("rcse_init_terminal_id",
        when($"rcse_init_terminal_id_conf_update".isNotNull, $"rcse_init_terminal_id_conf_update")
          .otherwise($"rcse_init_terminal_id"))
      .withColumn("rcse_init_terminal_sw_id",
        when($"rcse_init_terminal_sw_id_conf_update".isNotNull, $"rcse_init_terminal_sw_id_conf_update")
          .otherwise($"rcse_init_terminal_sw_id"))
      .withColumn("rcse_curr_client_id",
        when($"rcse_curr_client_id_conf_update".isNotNull, $"rcse_curr_client_id_conf_update")
          .otherwise($"rcse_curr_client_id"))
      .withColumn("rcse_curr_terminal_id",
        when($"rcse_curr_terminal_id_conf_update".isNotNull, $"rcse_curr_terminal_id_conf_update")
          .otherwise($"rcse_curr_terminal_id"))
      .withColumn("rcse_curr_terminal_sw_id",
        when($"rcse_curr_terminal_sw_id_conf_update".isNotNull, $"rcse_curr_terminal_sw_id_conf_update")
          .otherwise($"rcse_curr_terminal_sw_id"))
      .withColumn("modification_date",
        when($"modification_date_conf_update".isNotNull, $"modification_date_conf_update")
          .otherwise($"modification_date"))
      .select(
        outColumns.head, outColumns.tail :_*
      )


    val result = updJoin.union(umatched)

    result
      .coalesce(1)
      .write
      .option("delimiter", "|")
      .option("header", "false")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_f_rcse_conf.TMD.csv");

    logger.info(s"Conf file row count: ${result.count()}")
  }


}
