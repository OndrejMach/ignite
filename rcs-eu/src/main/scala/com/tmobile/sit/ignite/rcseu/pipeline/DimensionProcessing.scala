package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

trait DimensionProcessing extends Logger{
  def getNewUserAgents(activityData: DataFrame, registerRequestsData: DataFrame): DataFrame
}

class Dimension extends DimensionProcessing {
  // adding new user agents from today's activity and register requests data

  def processUserAgentsSCD(oldUserAgents: DataFrame, newUserAgents: DataFrame): DataFrame = {

    val max_id: Integer = oldUserAgents.select("_UserAgentID").orderBy(desc("_UserAgentID")).first().getInt(0)

    val fullUserAgents1 = newUserAgents.as("n")
      .filter("UserAgent is not null")
      .join(oldUserAgents.as("o"), lower(col("n.UserAgent")) === lower(col("o.UserAgent")), "leftanti")
      .withColumn("row_nr", row_number.over(Window.orderBy("UserAgent")))
      .withColumn("_UserAgentID", expr(s"$max_id + row_nr"))
      .drop("row_nr")

    logger.info(s"Detected ${fullUserAgents1.count} new user agents.")

    val fullUserAgents =
      fullUserAgents1
        .union(oldUserAgents)
        .cache()

    broadcast(fullUserAgents)
  }

  override def getNewUserAgents(activity: DataFrame, registerRequests: DataFrame): DataFrame = {
    // splitting user_agent data into the relevant columns
    /*
    UserAgent	=	Unique user agent
    OEM	=	OEM of the user agent
    Device	=	Device of the user agent
    Client	=	Client of the user agent
    FW	=	FW of the user agent
    Client_vs	=	Version of the client
    _UserAgentID	= generated	ID of the UserAgent

    */

   val activityandregistered = activity
      .select("user_agent")
      .distinct()
      .union(
        registerRequests
          .select("user_agent")
          .distinct()
      )
     .distinct()
     .sort("user_agent")
     .withColumnRenamed("user_agent","UserAgent")


    val dfUA1=activityandregistered.select("UserAgent").dropDuplicates()
      .withColumn("temp", split(col("UserAgent"), " "))
      .select(col("*") +: (0 until 4)
        .map(i => col("temp")
          .getItem(i)
          .as(s"col$i")): _*)
      .select("UserAgent","col1","col2","col3")
      .withColumn("temp", split(col("col1"), "/"))
      .select(col("*") +: (0 until 4)
        .map(i => col("temp")
          .getItem(i)
          .as(s"coll$i")): _*)
      .select("UserAgent","col2","col3","coll0","coll1")
      .withColumn("temp", split(col("col2"), "/"))
      .select(col("*") +: (0 until 2)
        .map(i => col("temp")
          .getItem(i)
          .as(s"colly$i")): _*)
      .select("UserAgent","col3","coll0","coll1","colly0","colly1")

    val cols = Map("num" -> 1, "letters" -> 2)
    val p = "(.*)-(.*)$"
    val dfUA22=cols.foldLeft(dfUA1){
      case (dfUA1, (colName, groupIdx)) => dfUA1.withColumn(colName, regexp_extract(col("coll1"), p, groupIdx))
    }.drop("coll1")

    val dfUA23=dfUA22
      //.withColumn("_UserAgentID", null)
      .withColumnRenamed("coll0", "OEM")
      .withColumnRenamed("letters", "FW")
      .withColumnRenamed("colly0", "Client")
      .withColumnRenamed("num", "Device")
      .withColumnRenamed("colly1", "Client_vs")
      .withColumn("Client_vs", concat_ws(" ",col("Client_vs"),col("col3")))
      .select("UserAgent","OEM","Device","Client","FW","Client_vs")
    dfUA23
  }

}