package com.tmobile.sit.ignite.common


import org.scalatest.{FlatSpec, FlatSpecLike, FunSuite}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SparkSessionProvider}
import com.tmobile.sit.ignite.common.readers.{CSVReader, ExcelReader, MSAccessReader}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
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

  "mdbReader" should "read mdb file properly" in {
    import spark.implicits._
    val msAccessReader = new MSAccessReader("src/test/resources/testData/E200804025_L134_201905.mdb", "E200804025_AGB_201905")
    val dfAccess =msAccessReader.read
    assertDataFrameEquals(dfAccess, ReferenceData.mdb_data_no_schema.toDF())
  }

  "mdbReader with schema" should "read mdb file properly" in {
    import spark.implicits._
    val schema =  StructType(
      StructField("BUCHMONAT", StringType, true) ::
      StructField("GESPR_DAUER", DoubleType, true) ::
      StructField("ANZ_VERB", DoubleType, true) ::
      StructField("EURO", DoubleType, true)::
      StructField("LEISTUNGSMONAT", StringType, true):: Nil)


    val msAccessReader = new MSAccessReader("src/test/resources/testData/E200804025_L134_201905.mdb", "E200804025_AGB_201905", Some(schema))
    val dfAccess =msAccessReader.read
    dfAccess.printSchema()
    dfAccess.show(false)
    assertDataFrameEquals(dfAccess, ReferenceData.mdb_data_with_schema.toDF())
  }


  "excelReaded" should "read a sheet in excel file properly" in {
    val excelReader = new ExcelReader("src/test/resources/testData/E200804025_L141_201912.xls", sheetName = "'E200804025_TS_201912'" )
    val xlsDF = excelReader.read()
    import spark.implicits._
    xlsDF.printSchema()

    assertDataFrameEquals(xlsDF, ReferenceData.excel_data.toDF)
  }

}