package com.tmobile.sit.ignite.inflight.processing.data

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.Column

/**
 * Final filter for getting data only for airlines we need
 */

object OutputFilters {
  def filterAirline(airlineCodes: Seq[String]): Column = {
    col("wlif_airline_code").isin(airlineCodes.map(lit(_)) :_*)}
}
