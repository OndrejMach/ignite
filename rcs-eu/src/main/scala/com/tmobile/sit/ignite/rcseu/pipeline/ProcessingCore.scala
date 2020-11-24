package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.data.{InputData, OutputData, PersistentData, PreprocessedData}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import com.tmobile.sit.ignite.rcseu.Application.date
import com.tmobile.sit.ignite.rcseu.Application.month
import com.tmobile.sit.ignite.rcseu.Application.year
import com.tmobile.sit.ignite.rcseu.Application.natcoNetwork
import com.tmobile.sit.ignite.rcseu.Application.isHistoric
import com.tmobile.sit.ignite.rcseu.Application.dayforkey
import com.tmobile.sit.ignite.rcseu.Application.monthforkey

trait ProcessingCore extends Logger{
  def process(inputData: InputData, preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

class Core extends ProcessingCore {

  override def process(inputData: InputData,stageData: PreprocessedData, persistentData: PersistentData): OutputData = {

    //stageData.activity.show()
    //stageData.provision.show()
    //stageData.registerRequests.show()

    //if isHistoric = true (if the config parameter is true)
    if (isHistoric) {
  //in Stage creating accumulators for activity data, provision data and register requests data
  val stage = new Stage()

  val acc_activity = stageData.activity
  acc_activity.cache()
  logger.info("Activity accumulator count: " + acc_activity.count())

  val acc_provision = stageData.provision
  acc_provision.cache()
  logger.info("Provision accumulator count: " + acc_provision.count())

  val acc_register_requests = stageData.registerRequests
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
      val provisionedDaily = null
      val provisionedMonthly = null
      val provisionedYearly = null
      val registeredDaily = null
      val registeredMonthly = null
      val registeredYearly = null
      val activeDaily = null
      val activeMonthly = null
      val activeYearly = null
      val serviceDaily = null



      OutputData(acc_activity,acc_provision,acc_register_requests,fullUserAgents,
        provisionedDaily,provisionedMonthly,provisionedYearly,
        registeredDaily,registeredMonthly,registeredYearly,
        activeDaily,activeMonthly,activeYearly,
        serviceDaily)
}
    else {

      //otherwise if isHistoric = false (if the config parameter is false) process all the data

      //in Stage creating accumulators for activity data, provision data and register requests data
      val stage = new Stage()

      val acc_activity = stageData.activity
      acc_activity.cache()
      logger.info("Activity accumulator count: " + acc_activity.count())

      val acc_provision = stageData.provision
      acc_provision.cache()
      logger.info("Provision accumulator count: " + acc_provision.count())

      val acc_register_requests = stageData.registerRequests
      acc_register_requests.cache()
      logger.info("Register requests accumulator count: " + acc_register_requests.count())


      //logic for UserAgents dimension, creating daily new file and replacing old one, adding only new user agents
      //TODO: change monotonically_increasing_id
      val dim = new Dimension()

      val newUserAgents = dim.getNewUserAgents(stageData.activity, stageData.registerRequests)

      val fullUserAgents0 = dim.processUserAgentsSCD(persistentData.oldUserAgents, newUserAgents).dropDuplicates("UserAgent")
      val fullUserAgents = fullUserAgents0.withColumn("_UserAgentID", monotonically_increasing_id)
      fullUserAgents.cache()
      logger.info("Full user agents count: " + fullUserAgents.count())


      // Processing facts, filtering data by date, month, year
      // and calling the FactsProcessing function on the filtered data
      //creating separate variable for each output
      val fact = new Facts()

      val filtered_daily_provision = persistentData.accumulated_provision.filter(col("FileDate").contains(date))
      val provisionedDaily = fact.getProvisionedDaily(filtered_daily_provision, dayforkey)
      logger.info("Provisioned daily count: " + provisionedDaily.count())

      val filtered_monthly_provision = persistentData.accumulated_provision.filter(col("FileDate").contains(month))
      val provisionedMonthly1 = fact.getProvisionedDaily(filtered_monthly_provision, monthforkey)
      val provisionedMonthly= provisionedMonthly1.withColumnRenamed("ConKeyP1","ConKeyP2")
          .withColumnRenamed("Provisioned_daily","Provisioned_monthly")
      logger.info("Provisioned monthly count: " + provisionedMonthly.count())

      val filtered_yearly_provision = persistentData.accumulated_provision.filter(col("FileDate").contains(year))
      val provisionedYearly1 = fact.getProvisionedDaily(filtered_yearly_provision, year)
      val provisionedYearly= provisionedYearly1.withColumnRenamed("ConKeyP1","ConKeyP3")
        .withColumnRenamed("Provisioned_daily","Provisioned_yearly")
      logger.info("Provisioned yearly count: " + provisionedYearly.count())

      /*val filtered_total_provision = persistentData.accumulated_provision.filter(col("FileDate").contains("20"))
      val provisionedTotal1 = fact.getProvisionedDaily(filtered_total_provision, "20")
      val provisionedTotal= provisionedTotal1.withColumnRenamed("ConKeyP1","ConKeyP4")
        .withColumnRenamed("Provisioned_daily","Provisioned_total")
      logger.info("Provisioned total count: " + provisionedTotal.count())*/
      //----------------------------------------------------------------------------

      val filtered_daily_register = persistentData.accumulated_register_requests.filter(col("FileDate").contains(date))
      val registeredDaily = fact.getRegisteredDaily(filtered_daily_register, fullUserAgents, dayforkey)
      logger.info("Registered daily count: " + registeredDaily.count())

      val filtered_monthly_register = persistentData.accumulated_register_requests.filter(col("FileDate").contains(month))
      val registeredMonthly1 = fact.getRegisteredDaily(filtered_monthly_register, fullUserAgents, monthforkey)
      val registeredMonthly= registeredMonthly1.withColumnRenamed("ConKeyR1","ConKeyR2")
        .withColumnRenamed("Registered_daily","Registered_monthly")
      logger.info("Registered monthly count: " + registeredMonthly.count())

      val filtered_yearly_register = persistentData.accumulated_register_requests.filter(col("FileDate").contains(year))
      val registeredYearly1 = fact.getRegisteredDaily(filtered_yearly_register, fullUserAgents, year)
      val registeredYearly= registeredYearly1.withColumnRenamed("ConKeyR1","ConKeyR3")
        .withColumnRenamed("Registered_daily","Registered_yearly")
      logger.info("Registered yearly count: " + registeredYearly.count())

      /*val filtered_total_register = persistentData.accumulated_register_requests.filter(col("FileDate").contains("20"))
      val registeredTotal1 = fact.getRegisteredDaily(filtered_total_register, fullUserAgents, "20")
      val registeredTotal= registeredTotal1.withColumnRenamed("ConKeyR1","ConKeyR4")
        .withColumnRenamed("Registered_daily","Registered_total")
      logger.info("Registered total count: " + registeredTotal.count())*/
      //----------------------------------------------------------------------------
      val filtered_daily_active = persistentData.accumulated_activity.filter(col("creation_date").contains(date))
      val activeDaily = fact.getActiveDaily(filtered_daily_active, fullUserAgents, dayforkey, natcoNetwork)
      logger.info("Active daily count: " + activeDaily.count())

      val filtered_monthly_active = persistentData.accumulated_activity.filter(col("creation_date").contains(month))
      val activeMonthly1 = fact.getActiveDaily(filtered_monthly_active, fullUserAgents, monthforkey, natcoNetwork)
      val activeMonthly= activeMonthly1.withColumnRenamed("ConKeyA1","ConKeyA2")
        .withColumnRenamed("Active_daily_succ_origterm", "Active_monthly_succ_origterm")
        .withColumnRenamed("Active_daily_succ_orig", "Active_monthly_succ_orig")
        .withColumnRenamed("Active_daily_unsucc_origterm", "Active_monthly_unsucc_origterm")
        .withColumnRenamed("Active_daily_unsucc_orig", "Active_monthly_unsucc_orig")
      logger.info("Active monthly count: " + activeMonthly.count())

      val filtered_yearly_active = persistentData.accumulated_activity.filter(col("creation_date").contains(year))
      val activeYearly1 = fact.getActiveDaily(filtered_yearly_active, fullUserAgents, year, natcoNetwork)
      val activeYearly= activeYearly1.withColumnRenamed("ConKeyR1","ConKeyR3")
        .withColumnRenamed("Active_daily_succ_origterm", "Active_yearly_succ_origterm")
        .withColumnRenamed("Active_daily_succ_orig", "Active_yearly_succ_orig")
        .withColumnRenamed("Active_daily_unsucc_origterm", "Active_yearly_unsucc_origterm")
        .withColumnRenamed("Active_daily_unsucc_orig", "Active_yearly_unsucc_orig")
      logger.info("Active yearly count: " + activeYearly.count())

     /* val filtered_total_active = persistentData.accumulated_activity.filter(col("creation_date").contains("20"))
      val activeTotal1 = fact.getActiveDaily(filtered_total_active, fullUserAgents, "20", natcoNetwork)
      val activeTotal= activeTotal1.withColumnRenamed("ConKeyR1","ConKeyR4")
      logger.info("Active total count: " + activeTotal.count())*/
      //----------------------------------------------------------------------------

      //calling ServiceFactsDaily function from FactsProcessing
      //generating one file each day (only daily processing needed)
      val serviceDaily = fact.getServiceFactsDaily(stageData.activity)
      logger.info("Service facts daily count: " + serviceDaily.count())



    OutputData(acc_activity,acc_provision,acc_register_requests,fullUserAgents,
      provisionedDaily,provisionedMonthly,provisionedYearly,
      registeredDaily,registeredMonthly,registeredYearly,
      activeDaily,activeMonthly,activeYearly,
      serviceDaily)
  }}
}