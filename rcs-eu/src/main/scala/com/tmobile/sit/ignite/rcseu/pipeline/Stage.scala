package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait StageProcessing extends Logger{
  def preprocessActivity(input: DataFrame) : DataFrame
  def preprocessProvision(input: DataFrame) : DataFrame
  def preprocessRegisterRequests(input: DataFrame) : DataFrame
}


class Stage extends StageProcessing {
  override def preprocessActivity(input: DataFrame): DataFrame = {
    input
  }

  override def preprocessProvision(input: DataFrame): DataFrame = {
    input
  }

  override def preprocessRegisterRequests(input: DataFrame): DataFrame = {
    input
  }
}
