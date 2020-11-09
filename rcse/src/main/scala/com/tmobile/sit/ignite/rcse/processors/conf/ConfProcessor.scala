package com.tmobile.sit.ignite.rcse.processors.conf

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.inputs.{ConfToStageInputs, LookupsData, LookupsDataReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first, lit, when, broadcast, max}

/**
 * Conf file processing. It takes incoming DM events and creates a new conf file. The intention is to create a new file for every new day.
 * @param inputs - input events + the actual conf file
 * @param lookups - lookups - tac, client, terminal..
 * @param max_Date - max date which actually indicates that row is still valid - this one is a far-future date
 * @param processing_date - input events processing date
 * @param sparkSession
 */

class ConfProcessor(inputs: ConfToStageInputs, lookups: LookupsData, max_Date: Date, processing_date: Date)(implicit sparkSession: SparkSession) extends Logger {

  import sparkSession.implicits._

  private lazy val outColumns = Seq("date_id", "natco_code",
    "msisdn", "rcse_tc_status_id",
    "rcse_init_client_id", "rcse_init_terminal_id", "rcse_init_terminal_sw_id",
    "rcse_curr_client_id", "rcse_curr_terminal_id",
    "rcse_curr_terminal_sw_id", "modification_date")

  private  val preprocessedTac = {
    logger.info("Preparing TAC data")
    logger.info(s"TAC RAW: ${lookups.tac.count()}")

    val terminalLookupTAC = lookups
      .terminal
      .filter($"tac_code".isNotNull)
      .select("tac_code", "rcse_terminal_id")
      .groupBy("tac_code")
      .agg(
        max("rcse_terminal_id").alias("rcse_terminal_id")
      )
      .groupBy("rcse_terminal_id")
      .agg(
        max("tac_code").alias("tac_code")
      )

    val terminalLookupID = lookups
      .terminal
      .select($"rcse_terminal_id", $"terminal_id".as("terminal_id_terminal"))
      .groupBy("terminal_id_terminal")
      .agg(
        max("rcse_terminal_id").alias("rcse_terminal_id")
      )

    val ret1 = lookups.tac
      .filter($"valid_to" >= lit(max_Date) && $"id".isNotNull)
      .join(terminalLookupTAC, Seq("tac_code"), "left_outer")
      .withColumnRenamed("rcse_terminal_id","rcse_terminal_id_tac" )
      .drop("rcse_terminal_id")

    val ret = ret1.join(terminalLookupID, $"terminal_id_terminal" === $"id", "left_outer")
      .withColumn("rcse_terminal_id_term", $"rcse_terminal_id")
      .drop("rcse_terminal_id", "terminal_id_terminal")
      .select($"tac_code", $"id".as("terminal_id"), $"rcse_terminal_id_tac", $"rcse_terminal_id_term".as("rcse_terminal_id_term"))
      .repartition(10)

    logger.info(s"TAC preproc: ${ret.count()}")

    //ret.summary().show(false)

    ret
  }

  private  val preprocessedEvents = {
    logger.info("Preprocessing Events data")
    logger.info(s"Events count RAW: ${inputs.events.count()}")

    val ret = inputs.events
      .drop("tac_code")
      .filter($"rcse_subscribed_status_id" === lit(1) && $"rcse_active_status_id" === lit(1))
      .join(broadcast(preprocessedTac), $"rcse_terminal_id_tac" === $"rcse_terminal_id", "left_outer")
      .withColumn("rcse_curr_terminal_id", when($"rcse_terminal_id_term".isNotNull, $"rcse_terminal_id_term").otherwise($"rcse_terminal_id"))
      .withColumn("rcse_curr_terminal_sw_id", $"rcse_terminal_sw_id")
      .withColumn("modification_date", $"date_id")
      .withColumn("rcse_init_client_id", $"rcse_client_id")
      .withColumn("rcse_init_terminal_id", $"rcse_terminal_id")
      .withColumn("rcse_init_terminal_sw_id", $"rcse_terminal_sw_id")
      .withColumn("rcse_curr_client_id", $"rcse_client_id")
      .na.fill("AB8A8EF1060EF8FE", Seq("msisdn"))
      .select(
        "date_id", "natco_code",
        "msisdn", "rcse_tc_status_id",
        "rcse_init_client_id", "rcse_init_terminal_id",
        "rcse_init_terminal_sw_id", "rcse_curr_client_id",
        "rcse_curr_terminal_id", "rcse_curr_terminal_sw_id",
        "modification_date"
      )
      .persist()

    logger.info(s"events preprocessed: ${ret.count()}")

    ret
  }

  private  val conf2 = {
    logger.info("Getting conf data with TAC")
    logger.info(s"OLD conf: ${inputs.confData.count()}")

    val terminalLookup = preprocessedTac.withColumn("e", lit(1)).select("rcse_terminal_id_tac", "rcse_terminal_id_term", "e")

    val ret1 = inputs
      .confData
      .join(
        broadcast(terminalLookup),
        $"rcse_curr_terminal_id" === $"rcse_terminal_id_tac", "left_outer")
      .withColumn("term", $"rcse_terminal_id_term")
      .filter($"term".isNotNull && $"e" === lit(1) &&
        (($"rcse_curr_terminal_id".isNotNull && $"term" =!= $"rcse_curr_terminal_id") ||
          $"rcse_curr_terminal_id".isNull))

    logger.info(s"Interim count ${ret1.count()}")

      val ret = ret1
      .withColumn("rcse_curr_terminal_id", $"term")
      .withColumn("modification_date", lit(processing_date))
      //.sort("msisdn")
      .groupBy("msisdn")
      .agg(
        first(outColumns.head).alias(outColumns.head),
        outColumns.tail.filter(_ != "msisdn").map(i => first(i).alias(i)): _*
      )
      .select(
        outColumns.head, outColumns.tail: _*
      )
      .persist()

    logger.info(s"conf enriched: ${ret.count()}")

    ret
  }

  private  val joinedEventsConfData = {
    logger.info("Addig Events")

    inputs.confData.filter("msisdn is null").show(false)

    val confColumns = inputs.confData.columns.map(_ + "_conf")
    val retu = preprocessedEvents
      .join(
        inputs.confData
          .toDF(confColumns: _*)
          .withColumn("e", lit(1)),
        $"msisdn" === $"msisdn_conf",
        "left"
      )
      .persist()

    logger.info(s"Joined events and conf: ${retu.count()}")

    retu
  }

  private  val umatched = {
    logger.info("Filtering unmatched rows")
    val filt  = joinedEventsConfData
      .filter($"e".isNull)


    filt.select("msisdn").distinct().show()

    logger.info(s"UnMatched filtered: ${filt.count()}")

     val ret= filt.select(outColumns.head, outColumns.tail : _*
        //outColumns.map(i => col(i + "_conf").as(i)): _*
      )
      .filter($"msisdn".isNotNull)

    logger.info(s"UnMatched count: ${ret.count()}")

    ret
  }


  private  val joined = {
    logger.info("Getting matched data")
    val ret  = joinedEventsConfData
      .filter($"e".isNotNull)
      .withColumn("modification_date", $"date_id")
      .withColumn("date_id", $"date_id_conf")
      .withColumn("rcse_tc_status_id", $"rcse_tc_status_id_conf")
      .withColumn("rcse_init_client_id", $"rcse_init_client_id_conf")
      .withColumn("rcse_init_terminal_id", $"rcse_init_terminal_id_conf")
      .withColumn("rcse_init_terminal_sw_id", $"rcse_init_terminal_sw_id_conf")
      .select(
        outColumns.head, outColumns.tail: _*
      )
      .persist()
    logger.info(s"Getting events and conf data matched: ${ret.count()}")
    ret
  }

  private  val updJoin = {
    val tmpUpdate = joined
      .union(conf2)
      .groupBy("msisdn")
      .agg(
        first(outColumns.head).alias(outColumns.head),
        outColumns.tail.filter(_ != "msisdn").map(i => first(i).alias(i)): _*
      )
        .persist()

    logger.info(s"tmpUpdate: ${tmpUpdate.count()}")

    logger.info("Calculating final output for matched data")

    val allCols = tmpUpdate.columns.map(_ + "_conf_update")

    val ret = inputs.confData
      .join(tmpUpdate.toDF(allCols: _*), $"msisdn" === $"msisdn_conf_update", "left_outer")
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
        outColumns.head, outColumns.tail: _*
      )

    logger.info(s"Final without unmatched ${ret.count()}")
    ret

  }


  val result = {
    logger.info(s"Unioning matched and unmatched to get final result, unmatched: ${umatched.count()}")
    updJoin.union(umatched)
  }

}
