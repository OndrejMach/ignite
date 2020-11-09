package com.tmobile.sit.ignite.rcse.processors.events

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.processors.Lookups
import com.tmobile.sit.ignite.rcse.processors.datastructures.EventsStage
import com.tmobile.sit.ignite.rcse.processors.inputs.{EventsInputData, LookupsData, LookupsDataReader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel

/**
 * Wrapper class for events processing outputs.
 * @param client - client list
 * @param terminal - list of the rcse terminals
 * @param terminalSW - list of the RCSE terminal SW versions
 * @param regDer - processed reg and der events
 * @param dm - processed DM events
 */
case class EventsOutput(client: DataFrame, terminal: DataFrame, terminalSW: DataFrame, regDer: DataFrame, dm: DataFrame, tac: DataFrame)

/**
 * Class performing events processing logic. First it enriches input events and then orchestrates calculation for client, terminal, terminalSW,
 * regDer and DM dimensions.
 *
 * @param inputData - input daily
 * @param lookups - lookups - actual client, terminal and terminalSW
 * @param load_date - used as a modification date in the tables
 * @param sparkSession
 */

class EventsProcessor(inputData: EventsInputData, lookups: LookupsData, load_date: Date)(implicit sparkSession: SparkSession) extends Logger{
  import sparkSession.implicits._

  val onlyMSISDNS = inputData.dataInput.select("msisdn")

  private val withLookups = {
    logger.info("Getting DM events")
    val dmEventsOnly = inputData.dataInput.filter($"rcse_event_type" === lit("DM"))

    logger.info("Calculating new events with Tac, terminal and client lookups")
    dmEventsOnly
      .withColumn("natco_code", lit("TMD"))
      .join(inputData.imsi3DesLookup, $"imsi" === $"number", "left_outer")
      .withColumn("imsi", $"des")
      .na.fill("#", Seq("msisdn"))
      .drop("des", "number")

      .withColumn("tac_code", when($"imei".isNotNull && length($"imei") > lit(8), trim($"imei").substr(0, 8)).otherwise($"imei"))
      .withColumn("client_vendor", upper($"client_vendor"))
      .withColumn("client_version", upper($"client_version"))
      .withColumn("terminal_vendor", upper($"terminal_vendor"))
      .withColumn("terminal_model", upper($"terminal_model"))
      .withColumn("terminal_sw_version", upper($"terminal_sw_version"))
      .tacLookup(lookups.tac)
      .clientLookup(lookups.client)
      .terminalLookup(lookups.terminal)
      .withColumn("rcse_terminal_id",
        when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal".cast(LongType))
          .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac".cast(LongType))
            .otherwise($"rcse_terminal_id_desc".cast(LongType))
          )
      )
      .drop("rcse_terminal_id_terminal", "rcse_terminal_id_tac", "rcse_terminal_id_desc")
      .terminalSWLookup(lookups.terminalSW)
      .sort($"msisdn".asc, $"date_id".asc)
      .groupBy("msisdn")
      .agg(
        max("date_id").alias("date_id"),
        (for (i <- EventsStage.withLookups if i != "msisdn" && i != "date_id" ) yield {
          last(i).alias(i)
        }): _*
      ).persist()
  }

  private val inputEventsRegDer = inputData.dataInput.filter($"rcse_event_type" =!= lit("DM"))


  def getDimensions: EventsOutput = {
    logger.info("Getting new data for terminaSW dimension")
    val terminalSW = new TerminalSWDimension(enrichedEvents = withLookups, oldTerminalSW = lookups.terminalSW, load_date = load_date).newTerminalSW.persist(StorageLevel.MEMORY_ONLY)
    logger.info("Getting new data for terminal dimension")
    val terminal = new TerminalDimension(enrichedEvents = withLookups, oldTerminal = lookups.terminal, tacData = lookups.tac, load_date = load_date).newTerminal.persist(StorageLevel.MEMORY_ONLY)
    logger.info("Getting new data for client dimension")
    val client = new ClientDimension(eventsEnriched = withLookups, clientsOld = lookups.client, load_date = load_date).newClient.persist(StorageLevel.MEMORY_ONLY)


    logger.info("Getting regDer output")
    val regDer = new RegDerDimension(inputEventsRegDer = inputEventsRegDer,
      msisdn3DesLookup = inputData.msisdn3DesLookup,
      imsi3DesLookup = inputData.imsi3DesLookup,
      client = client,
      terminal= terminal,
      tac = lookups.tac,
      terminalSW = terminalSW
    ).regDerOutput
      .persist()

    logger.info("Getting DM data")
    val dm = new DMDimension(eventInputsEnriched = withLookups,
      newClient = client,
      newTerminal = terminal,
      newTerminalSW = terminalSW,
      msisdn3DesLookup = inputData.msisdn3DesLookup)
      .eventsDM
      .persist()

    EventsOutput(client = client, terminal = terminal, terminalSW = terminalSW, regDer = regDer.persist(), dm = dm.persist(), tac = lookups.tac)
  }

}
