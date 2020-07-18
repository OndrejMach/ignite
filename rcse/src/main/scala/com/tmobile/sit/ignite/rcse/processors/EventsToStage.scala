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

/**
 * this one is responsible for the biggest processing - reads inputs and updates most of the data structures organised int the EventsOutput case class.
 * @param load_date - date for which data is processed
 * @param sparkSession
 * @param settings - paths to the input files
 */
class EventsToStage( load_date: Timestamp)(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

   def processData(): EventsOutput = {
    // input file reading

    val inputData = new EventsInputData()

    val lookups = new LookupsData()

    new EventsProcessor(inputData = inputData, lookups = lookups,load_date = Date.valueOf(load_date.toLocalDateTime.toLocalDate )).getDimensions

  }

}
