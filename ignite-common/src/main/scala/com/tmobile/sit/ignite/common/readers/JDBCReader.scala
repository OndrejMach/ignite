package com.tmobile.sit.ignite.common.readers

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

class JDBCReader(url: String, table: String, properties: Properties)(implicit sparkSession: SparkSession) extends Reader {
  def read() : DataFrame = {
    sparkSession
      .read
      //  .schema(???)
      .jdbc(url, table, properties)
  }
}
