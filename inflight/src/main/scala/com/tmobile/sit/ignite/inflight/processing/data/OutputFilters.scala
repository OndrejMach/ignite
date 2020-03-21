package com.tmobile.sit.ignite.inflight.processing.data

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.Column

object OutputFilters {
  val airlineCodes = Seq("DLH", "JAL","EVA","ETD","EIN","AAL","GIA","KLM","AFR","SIA","MXD","SCO","VIR","CAL","PAN")
  
  
  def filterAirline(): Column = {
    col("wlif_airline_code").isin(airlineCodes.map(lit(_)) :_*)}
}
