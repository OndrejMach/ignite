package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame

trait DimensionProcessing extends Logger{
  def getUserAgents(activityData: DataFrame, registerRequestsData: DataFrame): DataFrame
}

class Dimension extends DimensionProcessing {

  override def getUserAgents(activity: DataFrame, registerRequests: DataFrame): DataFrame = {
    //TODO: add logic here to split user_agents into parts
    activity
      .select("user_agent")
      .union(
        registerRequests
          .select("user_agent")
      )
      .distinct()
      .sort("user_agent")
  }
}