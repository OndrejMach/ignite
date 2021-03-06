package com.tmobile.sit.ignite.rcse

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.functions.{asc, broadcast, first, upper, last}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

package object processors {
  val MAX_DATE = Date.valueOf(LocalDate.of(4712, 12, 31))


  implicit class Lookups(df: DataFrame)(implicit sparkSession: SparkSession) {

    import sparkSession.implicits._

    def terminalSimpleLookup(terminal: DataFrame): DataFrame = {
      //terminal.printSchema()
      //df.printSchema()
      df
        .join(
          terminal
            .filter($"terminal_id".isNotNull)
          //  .sort(asc("modification_date"))
            .groupBy($"terminal_id")
            .agg(last("rcse_terminal_id").alias("rcse_terminal_id_terminal"))
            .persist(StorageLevel.MEMORY_ONLY)
        , Seq("terminal_id"), "left_outer")
        .join(
          terminal
            .filter($"tac_code".isNotNull)
          //  .sort(asc("modification_date"))
            .groupBy("tac_code")
            .agg(last("rcse_terminal_id").alias("rcse_terminal_id_tac"))
            .persist(StorageLevel.MEMORY_ONLY)
        , Seq("tac_code"), "left_outer")
    }

    def terminalDescLookup(terminal: DataFrame): DataFrame = {
      df
        .join(
          (terminal
           // .sort(asc("modification_date"))
            .groupBy($"rcse_terminal_vendor_sdesc", $"rcse_terminal_model_sdesc")
            .agg(last("rcse_terminal_id").alias("rcse_terminal_id_desc")).persist(StorageLevel.MEMORY_ONLY)),
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
        .join((tacTerminal.select("tac_code", "terminal_id").distinct().persist(StorageLevel.MEMORY_ONLY)), Seq("tac_code"), "left_outer")
    }

    def terminalSWLookup(terminalSW: DataFrame): DataFrame = {
      df
        .join((terminalSW.select("rcse_terminal_sw_id", "rcse_terminal_sw_desc").distinct().persist(StorageLevel.MEMORY_ONLY)),
          upper($"terminal_sw_version") === upper($"rcse_terminal_sw_desc"), "left_outer")
        .drop("rcse_terminal_sw_desc")
    }

    def clientLookup(client: DataFrame): DataFrame = {
      df
        .join((client.select("rcse_client_id", "rcse_client_vendor_sdesc", "rcse_client_version_sdesc").distinct().persist(StorageLevel.MEMORY_ONLY)),
          $"rcse_client_vendor_sdesc" === $"client_vendor" && $"rcse_client_version_sdesc" === $"client_version", "left_outer")
        .drop("rcse_client_vendor_sdesc", "rcse_client_version_sdesc")
    }
  }

}

