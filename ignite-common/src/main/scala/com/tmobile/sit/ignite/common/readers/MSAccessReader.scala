package com.tmobile.sit.ignite.common.readers

import java.sql.{DriverManager, ResultSet}

import com.tmobile.sit.ignite.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

trait AccessFileReader extends Logger{
  def getData() : DataFrame
}

///Users/ondrej.machacek/Downloads/List_Of_Managers.accdb
class ListOfManagersReader(path: String, columnNames: Seq[String])(implicit sparkSession: SparkSession) extends Reader {
  private val conn = DriverManager.getConnection(s"jdbc:ucanaccess://${path}")

  private def resultSetToDataFrame(resultSet: ResultSet, schema: Seq[String])(implicit sparkSession: SparkSession): DataFrame = {

    val columnCount = resultSet.getMetaData.getColumnCount
    val resultSetAsList: List[Seq[String]] = new Iterator[Seq[String]] {
      override def hasNext: Boolean = resultSet.next()

      override def next(): Seq[String] = {
        for {i<-1 to columnCount} yield {resultSet.getString(i)}
      } //{
    }.toStream.toList

    import sparkSession.implicits._
    resultSetAsList.toDF("values").select((0 until columnCount).map(i => $"values".getItem(i).as(schema(i))): _*)
  }


  val read: DataFrame = {
    logger.info(s"Getting data from file ${path}")
    val st = conn.createStatement
    val rs = st.executeQuery("SELECT * FROM Managers")

    val schema = Seq("Code", "City", "Manager")
    logger.info(s"converting resultset into dataframe with schema ${schema}")
    resultSetToDataFrame(rs, columnNames)
  }
}

