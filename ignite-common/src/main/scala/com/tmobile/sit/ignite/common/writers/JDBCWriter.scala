package com.tmobile.sit.ignite.common.writers

import java.util.Properties

import org.apache.spark.sql.DataFrame

class JDBCWriter(url: String, table: String, connectionProperties: Properties, data: DataFrame) extends Writer {
  def writeData(): Unit = {
    logger.info(s"Writing via JDBC to ${url} table: ${table}")
    data
      .write
      .jdbc(url, table, connectionProperties)
  }
}
