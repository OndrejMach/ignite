package com.tmobile.sit.ignite.common


import org.scalatest.{FlatSpec, FlatSpecLike, FunSuite}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SparkSessionProvider}
import com.tmobile.sit.ignite.common.readers.CSVReader
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class ReadersTest extends FlatSpec with DataFrameSuiteBase  {
  implicit lazy val _ : SparkSession = spark

  "csvReader" should "read csv with header" in {
    import spark.implicits._

    val csvReader = new CSVReader("src/test/resources/testData/testCsv.csv", true)
    val df = csvReader.read()
    val refDF = ReferenceData.csv_with_header.toDF
    assertDataFrameEquals(df, refDF) // equal
/*
    val input2 = List(4, 5, 6).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input1, input2) // not equal
    }

 */
  }



}