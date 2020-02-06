package com.tmobile.sit.ignite.common

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

final case class InputRow(id: Option[Int], number: Option[Int], string: Option[Int])

object ReferenceData {

  def  csv_with_header = List( InputRow(Some(1),Some(2),Some(3)))
}
