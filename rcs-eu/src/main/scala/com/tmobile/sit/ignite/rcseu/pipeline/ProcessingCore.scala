package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.data.{OutputData, PersistentData, PreprocessedData}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import com.tmobile.sit.ignite.rcseu.Application.date
import com.tmobile.sit.ignite.rcseu.Application.month
import com.tmobile.sit.ignite.rcseu.Application.year



trait ProcessingCore extends Logger{
  def process(preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

class Core extends ProcessingCore {

  override def process(stageData: PreprocessedData, persistentData: PersistentData): OutputData = {

    //stageData.activity.show()
    //stageData.provision.show()
    //stageData.registerRequests.show()

    val stage = new Stage()

    val acc_activity = stage.preprocessActivity(stageData.activity,persistentData.accumulated_activity)
    logger.info("Activity accumulator count: " + acc_activity.count())

    val acc_provision = stage.preprocessProvision(stageData.provision,persistentData.accumulated_provision)
    logger.info("Provision accumulator count: " + acc_provision.count())

    val acc_register_requests = stage.preprocessRegisterRequests(stageData.registerRequests,persistentData.accumulated_register_requests)
    logger.info("Register requests accumulator count: " + acc_register_requests.count())


    val dim = new Dimension()

    // logic for UserAgents dimension
    val newUserAgents = dim.getNewUserAgents(stageData.activity, stageData.registerRequests)

    val fullUserAgents0 = dim.processUserAgentsSCD(persistentData.oldUserAgents, newUserAgents).dropDuplicates("UserAgent")
    val fullUserAgents =fullUserAgents0.withColumn("_UserAgentID", monotonically_increasing_id)
    fullUserAgents.cache()
    logger.info("Full user agents count: " + fullUserAgents.count())

    // Process facts
    val fact = new Facts()

    //val filtered_monthly_provision = persistentData.accumulated_provision.filter(col("FileDate").startsWith("2020-02"))
    //val provisionedMonthly = fact.getProvisionedDaily(filtered_monthly_provision)
    //logger.info("Provisioned monthly count: " + provisionedMonthly.count())


    val filtered_daily_provision= persistentData.accumulated_provision.filter(col("FileDate").contains(date))
    val provisionedDaily = fact.getProvisionedDaily(filtered_daily_provision,date)
    logger.info("Provisioned daily count: " + provisionedDaily.count())

    val filtered_monthly_provision= persistentData.accumulated_provision.filter(col("FileDate").contains(month))
    val provisionedMonthly = fact.getProvisionedDaily(filtered_monthly_provision,month)
    logger.info("Provisioned monthly count: " + provisionedMonthly.count())

    val filtered_yearly_provision= persistentData.accumulated_provision.filter(col("FileDate").contains(year))
    val provisionedYearly = fact.getProvisionedDaily(filtered_yearly_provision,year)
    logger.info("Provisioned yearly count: " + provisionedYearly.count())

    val filtered_total_provision= persistentData.accumulated_provision.filter(col("FileDate").contains("20"))
    val provisionedTotal = fact.getProvisionedDaily(filtered_total_provision,"20")
    logger.info("Provisioned total count: " + provisionedTotal.count())
    //----------------------------------------------------------------------------

    val filtered_daily_register= persistentData.accumulated_register_requests.filter(col("FileDate").contains(month))
    val registeredDaily = fact.getRegisteredDaily(filtered_daily_register,fullUserAgents,month)
    logger.info("Registered daily count: " + registeredDaily.count())


    val filtered_daily_active= persistentData.accumulated_activity.filter(col("creation_date").contains(month))
    val activeDaily = fact.getActiveDaily(filtered_daily_active,fullUserAgents,month)
    logger.info("Active daily count: " + activeDaily.count())

    val serviceDaily = fact.getServiceFactsDaily(stageData.activity)
    logger.info("Service facts daily count: " + activeDaily.count())


    OutputData(acc_activity,acc_provision,acc_register_requests,fullUserAgents,
      provisionedDaily,provisionedMonthly,provisionedYearly,provisionedTotal,
      registeredDaily,
      activeDaily,serviceDaily)
  }
}