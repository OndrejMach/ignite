package com.tmobile.sit.ignite.rcse.processors

import java.sql.{Date, Timestamp}

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.datastructures.EventsStage
import com.tmobile.sit.ignite.rcse.processors.events.EventsProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{EventsInputData, LookupsData}
import com.tmobile.sit.ignite.rcse.processors.udfs.UDFs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
lookups:
- client: EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_client.csv
- TAC: $EWHR_COMMON_STAGE_DIR/cptm_ta_d_tac.csv
- terminal: $EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_terminal.csv
- terminal_sw: $EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_terminal_sw.csv
- input events: TMD_{HcsRcsDwh_m4sxvmvsm6h?,RegAsDwh_Aggregate}_$ODATE.csv
 */


class EventsToStage(settings: Settings, load_date: Timestamp)(implicit sparkSession: SparkSession) extends Processor {

  override def processData(): Unit = {
    // input file reading
    import sparkSession.implicits._

    val inputData = new EventsInputData(settings)

    val lookups = new LookupsData(settings = settings)

    val events = new EventsProcessor(inputData = inputData, lookups = lookups,load_date = Date.valueOf(load_date.toLocalDateTime.toLocalDate ))
/*

    logger.info(s"Getting new DM file, row count ${output.count()}")
    output
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "true")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_f_rcse_events.TMD.20200607.dm.csv");

 */
  }

}
