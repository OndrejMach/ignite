package com.tmobile.sit.ignite.common.readers

import java.sql.{DriverManager, ResultSet}

import com.tmobile.sit.ignite.common.Logger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

trait AccessFileReader extends Logger {
  def getData(): DataFrame
}

/*
question is whether to return typed structures, at the moment all is typed to String
 */
class MSAccessReader(path: String, tableName: String, schema : Option[StructType] = None)(implicit sparkSession: SparkSession) extends Reader {
  private val conn = DriverManager.getConnection(s"jdbc:ucanaccess://${path}")

  private def cast(data: Seq[String]) : Row = {
    val typedSeq = for {i<- 0 to (data.length-1)} yield {
      schema.get.fields(i).dataType match  {
        case DoubleType => data(i).toDouble
        case IntegerType => data(i).toInt
        case _ => data(i)
      }
    }
    Row.fromSeq(typedSeq)
  }

  private def resultSetToDataFrame(resultSet: ResultSet)(implicit sparkSession: SparkSession): DataFrame = {

    val columnCount = resultSet.getMetaData.getColumnCount
    val columnNames = for {i <- 1 to columnCount} yield resultSet.getMetaData.getColumnName(i)

    val resultSetAsList: List[Seq[String]] = new Iterator[Seq[String]] {
      override def hasNext: Boolean = resultSet.next()

      override def next(): Seq[String] = {
         for {i <- 1 to columnCount} yield {
          resultSet.getString(i)
        }
      } //{
    }.toStream.toList

    import sparkSession.implicits._

    if (!schema.isDefined) {
      resultSetAsList.toDF("values").select((0 until columnCount).map(i => $"values".getItem(i).as(columnNames(i))): _*)
    } else {
      val data = resultSetAsList
      if (data.length != schema.get.length) {
        logger.error(s"Schema does not contain the same number of fields as read MS Access file (data: ${data.length}, schema: ${schema.get.length})!!, returning empty DF")
        sparkSession.emptyDataFrame
      } else {
        logger.info("Trying to cast MS Access data to required schema")
        val toRows = data.map(r => cast(r))
        sparkSession.createDataFrame(toRows.asJava, schema.get)
      }
    }
  }


  val read: DataFrame = {
    logger.info(s"Getting data from file ${path}, table: ${tableName}")
    val st = conn.createStatement
    val rs = st.executeQuery(s"SELECT * FROM ${tableName}")

    //val schema = Seq("Code", "City", "Manager")
    logger.info(s"converting resultset to dataframe")
    resultSetToDataFrame(rs)

  }
}

