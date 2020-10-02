package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.data.{OutputData, PersistentData, PreprocessedData}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import com.tmobile.sit.ignite.rcseu.Application.date
import com.tmobile.sit.ignite.rcseu.Application.month
import com.tmobile.sit.ignite.rcseu.Application.year
import com.tmobile.sit.ignite.rcseu.Application.natcoNetwork



trait ProcessingCore extends Logger{
  def process(preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

class Core extends ProcessingCore {

  override def process(stageData: PreprocessedData, persistentData: PersistentData): OutputData = {

    //stageData.activity.show()
    //stageData.provision.show()
    //stageData.registerRequests.show()


    //in Stage creating accumulators for activity data, provision data and register requests data
    val stage = new Stage()

    val acc_activity = stage.preprocessActivity(stageData.activity,persistentData.accumulated_activity)
    acc_activity.cache()
    logger.info("Activity accumulator count: " + acc_activity.count())

    val acc_provision = stage.preprocessProvision(stageData.provision,persistentData.accumulated_provision)
    acc_provision.cache()
    logger.info("Provision accumulator count: " + acc_provision.count())

    val acc_register_requests = stage.preprocessRegisterRequests(stageData.registerRequests,persistentData.accumulated_register_requests)
    acc_register_requests.cache()
    logger.info("Register requests accumulator count: " + acc_register_requests.count())


    //logic for UserAgents dimension, creating daily new file and replacing old one, adding only new user agents
   //TODO: change monotonically_increasing_id
    val dim = new Dimension()

    val newUserAgents = dim.getNewUserAgents(stageData.activity, stageData.registerRequests)

    val fullUserAgents0 = dim.processUserAgentsSCD(persistentData.oldUserAgents, newUserAgents).dropDuplicates("UserAgent")
    val fullUserAgents =fullUserAgents0.withColumn("_UserAgentID", monotonically_increasing_id)
    fullUserAgents.cache()
    logger.info("Full user agents count: " + fullUserAgents.count())


    // Processing facts, filtering data by date, month, year
    // and calling the FactsProcessing function on the filtered data
    //creating separate variable for each output
    val fact = new Facts()

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

    val filtered_daily_register= persistentData.accumulated_register_requests.filter(col("FileDate").contains(date))
    val registeredDaily = fact.getRegisteredDaily(filtered_daily_register,fullUserAgents,date)
    logger.info("Registered daily count: " + registeredDaily.count())

    val filtered_monthly_register= persistentData.accumulated_register_requests.filter(col("FileDate").contains(month))
    val registeredMonthly = fact.getRegisteredDaily(filtered_monthly_register,fullUserAgents,month)
    logger.info("Registered monthly count: " + registeredMonthly.count())

    val filtered_yearly_register= persistentData.accumulated_register_requests.filter(col("FileDate").contains(year))
    val registeredYearly = fact.getRegisteredDaily(filtered_yearly_register,fullUserAgents,year)
    logger.info("Registered yearly count: " + registeredYearly.count())

    val filtered_total_register= persistentData.accumulated_register_requests.filter(col("FileDate").contains("20"))
    val registeredTotal = fact.getRegisteredDaily(filtered_total_register,fullUserAgents,"20")
    logger.info("Registered total count: " + registeredTotal.count())
    //----------------------------------------------------------------------------
    val filtered_daily_active= persistentData.accumulated_activity.filter(col("creation_date").contains(date))
    val activeDaily = fact.getActiveDaily(filtered_daily_active,fullUserAgents,date,natcoNetwork)
    logger.info("Active daily count: " + activeDaily.count())

    val filtered_monthly_active= persistentData.accumulated_activity.filter(col("creation_date").contains(month))
    val activeMonthly = fact.getActiveDaily(filtered_monthly_active,fullUserAgents,month,natcoNetwork)
    logger.info("Active monthly count: " + activeMonthly.count())

    val filtered_yearly_active= persistentData.accumulated_activity.filter(col("creation_date").contains(year))
    val activeYearly = fact.getActiveDaily(filtered_yearly_active,fullUserAgents,year,natcoNetwork)
    logger.info("Active yearly count: " + activeYearly.count())

    val filtered_total_active= persistentData.accumulated_activity.filter(col("creation_date").contains("20"))
    val activeTotal = fact.getActiveDaily(filtered_total_active,fullUserAgents,"20",natcoNetwork)
    logger.info("Active total count: " + activeTotal.count())
//----------------------------------------------------------------------------

    //calling ServiceFactsDaily function from FactsProcessing
    //generating one file each day (only daily processing needed)
    val serviceDaily = fact.getServiceFactsDaily(stageData.activity)
    logger.info("Service facts daily count: " + serviceDaily.count())


    OutputData(acc_activity,acc_provision,acc_register_requests,fullUserAgents,
      provisionedDaily,provisionedMonthly,provisionedYearly,provisionedTotal,
      registeredDaily,registeredMonthly,registeredYearly,registeredTotal,
      activeDaily,activeMonthly,activeYearly,activeTotal,
      serviceDaily)
  }
}