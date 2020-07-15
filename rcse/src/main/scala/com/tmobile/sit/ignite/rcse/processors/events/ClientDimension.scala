package com.tmobile.sit.ignite.rcse.processors.events

import java.sql.Date

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, first, lit, max, monotonically_increasing_id}

class ClientDimension(eventsEnriched: DataFrame, clientsOld: DataFrame, load_date: Date)(implicit sparkSession: SparkSession) extends Logger{
  val newClient = {
    import sparkSession.implicits._
    logger.info("Getting current Max client ID")
    val clientMax = clientsOld.select(max("rcse_client_id")).collect()(0).getInt(0)


    logger.info("Getting new clients and assignning them with new IDs")
    val dimensionA =
      eventsEnriched
        .filter($"rcse_client_id".isNull)
        .select(
          lit(-1).as("rcse_client_id"),
          $"client_vendor".as("rcse_client_vendor_sdesc"),
          $"client_vendor".as("rcse_client_vendor_ldesc"),
          $"client_version".as("rcse_client_version_sdesc"),
          $"client_version".as("rcse_client_version_ldesc"),
          lit(load_date).as("modification_date"))
        .sort(desc("rcse_client_vendor_sdesc"), desc("rcse_client_version_sdesc"), desc("modification_date"))
        .groupBy("rcse_client_vendor_sdesc", "rcse_client_version_sdesc")
        .agg(
          first("rcse_client_id").alias("rcse_client_id"),
          first("rcse_client_vendor_ldesc").alias("rcse_client_vendor_ldesc"),
          first("rcse_client_version_ldesc").alias("rcse_client_version_ldesc"),
          first("modification_date").alias("modification_date")
        )
        .withColumn("rcse_client_id", monotonically_increasing_id() + lit(clientMax))

    logger.info("Unioning old clients with the new ones")
    clientsOld
      .drop("entry_id", "load_date")
      .union(dimensionA)
  }
}
