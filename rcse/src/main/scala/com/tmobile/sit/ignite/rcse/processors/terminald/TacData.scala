package com.tmobile.sit.ignite.rcse.processors.terminald

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions.{count, first, length, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class containes preprocessed TAC data
 * @param tac - actual tac data
 * @param maxDate - date used for the upper validity date limit for actually valid rows
 * @param sparkSession
 */

class TacData(tac: DataFrame, maxDate: java.sql.Date)(implicit sparkSession: SparkSession) extends Logger {
  import sparkSession.implicits._

  private val tacFiltered = {
    logger.info("Filtering Tac data for only the actual one")
    tac.filter($"valid_to" >= lit(maxDate) && $"status".isNotNull &&
      ($"status" === lit("ACTIVE") ||
        ($"status".substr(0, 2) === lit("NO") &&
          ($"status".substr(length($"status") - lit(4), lit(4)) === lit("INFO"))
          )
        )
    )
      .select($"terminal_id", $"tac_code", $"id", $"manufacturer", $"model", $"load_date")
  }

   val tacProcessed = {
     logger.info("Preparing final TAC data")
     tacFiltered
       .select($"terminal_id", $"tac_code".substr(0, 6).as("tac_code"), $"id", $"manufacturer", $"model", $"load_date")
       .distinct()
       .groupBy("tac_code")
       .agg(
         count("*").alias("count"),
         first("terminal_id").alias("terminal_id"),
         first("id").alias("id"),
         first("manufacturer").alias("manufacturer"),
         first("model").alias("model"),
         first("load_date").alias("load_date")
       )
       .filter($"count" === lit(1))
       .drop("count")
       .union(tacFiltered)
       .drop("terminal_id")
   }
}
