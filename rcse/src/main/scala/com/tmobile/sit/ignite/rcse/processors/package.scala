package com.tmobile.sit.ignite.rcse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast

package object processors {

  implicit class Lookups(df: DataFrame)(implicit sparkSession: SparkSession) {

    import sparkSession.implicits._

    def terminalLookup(terminal: DataFrame): DataFrame = {
      df
        .join(broadcast(terminal.select($"rcse_terminal_id".as("rcse_terminal_id_terminal"), $"terminal_id")), Seq("terminal_id"), "left_outer")
        .join(broadcast(terminal.select($"tac_code", $"rcse_terminal_id".as("rcse_terminal_id_tac")).sort().distinct()), Seq("tac_code"), "left_outer")
        .join(broadcast(terminal.select($"rcse_terminal_vendor_sdesc", $"rcse_terminal_model_sdesc", $"rcse_terminal_id".as("rcse_terminal_id_desc"))),
          $"terminal_vendor" === $"rcse_terminal_vendor_sdesc" && $"rcse_terminal_model_sdesc" === $"terminal_model", "left_outer")
        .drop("rcse_terminal_vendor_sdesc", "rcse_terminal_model_sdesc")
    }

    def tacLookup(tacTerminal: DataFrame): DataFrame = {
      df
        .join(broadcast(tacTerminal.select("tac_code", "terminal_id")), Seq("tac_code"), "left_outer")
    }

    def terminalSWLookup(terminalSW: DataFrame): DataFrame = {
      df
        .join(broadcast(terminalSW.select("rcse_terminal_sw_id", "rcse_terminal_sw_desc")), $"terminal_sw_version" === $"rcse_terminal_sw_desc", "left_outer")
        .drop("rcse_terminal_sw_desc")
    }
    def clientLookup(client: DataFrame): DataFrame = {
      df
      .join(broadcast(client.select("rcse_client_id", "rcse_client_vendor_sdesc", "rcse_client_version_sdesc")),
        $"rcse_client_vendor_sdesc" === $"client_vendor" && $"rcse_client_version_sdesc" === $"client_version", "left_outer")
        .drop("rcse_client_vendor_sdesc", "rcse_client_version_sdesc")
    }
  }

}

