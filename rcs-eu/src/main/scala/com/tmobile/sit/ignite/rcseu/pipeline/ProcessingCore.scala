package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

trait ProcessingCore {
  def process(preprocessedData: PreprocessedData) : DataFrame
}

class CoreLogicWithTransform extends ProcessingCore {
  /*
  private def joinPeopleAndSalaryInfo(salaryInfo: DataFrame)(people: DataFrame) = {
    people.join(salaryInfo,Seq("id"), "inner")
  }
  private def aggregateOnSalary(peopleWithSalary: DataFrame) : DataFrame = {
    peopleWithSalary
      .groupBy("salary", "address")
      .agg(count("salary")
        .alias("count"))
  }
 */


  override def process(preprocessedData: PreprocessedData): DataFrame = {

    preprocessedData.activityData.show()
    preprocessedData.provisionData.show()
    preprocessedData.registerRequestsData.show()

    preprocessedData.activityData
  }
}