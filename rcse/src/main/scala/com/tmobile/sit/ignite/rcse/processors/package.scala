package com.tmobile.sit.ignite.rcse

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, upper, min}

package object processors {
  val MAX_DATE = Date.valueOf(LocalDate.of(4712, 12, 31))


  implicit class Lookups(df: DataFrame)(implicit sparkSession: SparkSession) {

    import sparkSession.implicits._

    def terminalSimpleLookup(terminal: DataFrame): DataFrame = {
      df
        .join(broadcast(
          terminal
            .filter($"terminal_id".isNotNull)
            .groupBy($"terminal_id")
            .agg(min("rcse_terminal_id").alias("rcse_terminal_id_terminal")).cache()
        ), Seq("terminal_id"), "left_outer")
        .join(broadcast(
          terminal
            .filter($"tac_code".isNotNull)
            .groupBy("tac_code")
            .agg(min("rcse_terminal_id").alias("rcse_terminal_id_tac")).cache()
        ), Seq("tac_code"), "left_outer")
    }

    def terminalDescLookup(terminal: DataFrame): DataFrame = {
      df
        .join(broadcast(
          terminal
            .groupBy($"rcse_terminal_vendor_sdesc", $"rcse_terminal_model_sdesc")
            .agg(min("rcse_terminal_id").alias("rcse_terminal_id_desc")).cache()),
          $"terminal_vendor" === $"rcse_terminal_vendor_sdesc" && $"rcse_terminal_model_sdesc" === $"terminal_model", "left_outer")
        .drop("rcse_terminal_vendor_sdesc", "rcse_terminal_model_sdesc")
    }

    def terminalLookup(terminal: DataFrame): DataFrame = {
      df
        .terminalSimpleLookup(terminal)
        .terminalDescLookup(terminal)
    }

    def tacLookup(tacTerminal: DataFrame): DataFrame = {
      df
        .join(broadcast(tacTerminal.select("tac_code", "terminal_id").distinct()).cache(), Seq("tac_code"), "left_outer")
    }

    def terminalSWLookup(terminalSW: DataFrame): DataFrame = {
      df
        .join(broadcast(terminalSW.select("rcse_terminal_sw_id", "rcse_terminal_sw_desc").distinct()).cache(), upper($"terminal_sw_version") === upper($"rcse_terminal_sw_desc"), "left_outer")
        .drop("rcse_terminal_sw_desc")
    }

    def clientLookup(client: DataFrame): DataFrame = {
      df
        .join(broadcast(client.select("rcse_client_id", "rcse_client_vendor_sdesc", "rcse_client_version_sdesc").distinct()).cache(),
          $"rcse_client_vendor_sdesc" === $"client_vendor" && $"rcse_client_version_sdesc" === $"client_version", "left_outer")
        .drop("rcse_client_vendor_sdesc", "rcse_client_version_sdesc")
    }
  }

}

