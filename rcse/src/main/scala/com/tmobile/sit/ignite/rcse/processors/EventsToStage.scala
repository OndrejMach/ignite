package com.tmobile.sit.ignite.rcse.processors

import java.sql.{Date, Timestamp}

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.events.{EventsOutput, EventsProcessor}
import com.tmobile.sit.ignite.rcse.processors.inputs.{EventsInputData, LookupsData}
import org.apache.spark.sql.SparkSession

/*
lookups:
- client: EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_client.csv
- TAC: $EWHR_COMMON_STAGE_DIR/cptm_ta_d_tac.csv
- terminal: $EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_terminal.csv
- terminal_sw: $EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_terminal_sw.csv
- input events: TMD_{HcsRcsDwh_m4sxvmvsm6h?,RegAsDwh_Aggregate}_$ODATE.csv
 */


class EventsToStage( load_date: Timestamp)(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

   def processData(): EventsOutput = {
    // input file reading

    val inputData = new EventsInputData()

    val lookups = new LookupsData()

    new EventsProcessor(inputData = inputData, lookups = lookups,load_date = Date.valueOf(load_date.toLocalDateTime.toLocalDate )).getDimensions
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
