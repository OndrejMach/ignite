package com.tmobile.sit.ignite.rcseu.pipeline

//import breeze.linalg.split
import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait DimensionProcessing extends Logger{
  def getNewUserAgents(activityData: DataFrame, registerRequestsData: DataFrame): DataFrame
}

class Dimension extends DimensionProcessing {

  def processUserAgentsSCD(oldUserAgents: DataFrame, newUserAgents: DataFrame): DataFrame = {
    oldUserAgents
      .union(newUserAgents)
      .distinct()

  }

  override def getNewUserAgents(activity: DataFrame, registerRequests: DataFrame): DataFrame = {
    //TODO: add logic here to split user_agents into parts
   val activityandregistered = activity
      .select("user_agent")
      .union(
        registerRequests
          .select("user_agent")
      )
      .distinct()
      .sort("user_agent")

    val dfUA1=activityandregistered.select("user_agent")
      .withColumn("temp", split(col("user_agent"), " "))
      .select(col("*") +: (0 until 4)
        .map(i => col("temp")
          .getItem(i)
          .as(s"col$i")): _*)
      .select("user_agent","col1","col2","col3")
      .withColumn("temp", split(col("col1"), "/"))
      .select(col("*") +: (0 until 4)
        .map(i => col("temp")
          .getItem(i)
          .as(s"coll$i")): _*)
      .select("user_agent","col2","col3","coll0","coll1")
      .withColumn("temp", split(col("col2"), "/"))
      .select(col("*") +: (0 until 2)
        .map(i => col("temp")
          .getItem(i)
          .as(s"colly$i")): _*)
      .select("user_agent","col3","coll0","coll1","colly0","colly1")

    val cols = Map("num" -> 1, "letters" -> 2)
    val p = "(.*)-(.*)$"
    val dfUA22=cols.foldLeft(dfUA1){
      case (dfUA1, (colName, groupIdx)) => dfUA1.withColumn(colName, regexp_extract(col("coll1"), p, groupIdx))
    }.drop("coll1")

    val dfUA23=dfUA22
      .withColumnRenamed("coll0", "OEM")
      .withColumnRenamed("letters", "FW")
      .withColumnRenamed("colly0", "Client")
      .withColumnRenamed("num", "Device")
      .withColumnRenamed("colly1", "Client_vs")
      .withColumn("Client_vs", concat_ws(" ",col("Client_vs"),col("col3")))
      .select("user_agent","OEM","Device","Client","FW","Client_vs")
    dfUA23
  }
}