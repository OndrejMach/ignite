package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import com.tmobile.sit.ignite.rcseu.data.{InputDataProvider, PersistentDataProvider}
import com.tmobile.sit.ignite.rcseu.pipeline._
import org.apache.spark.sql.SparkSession


object RunMode extends Enumeration {
  type RunMode = Value

  val YEARLY, DAILY, UPDATE, EOY = Value
}

object Application extends App with Logger {
  require(args.length == 3, "Wrong arguments. Usage: ... <date:yyyy-mm-dd> <natco:mt|cg|st|cr|mk> <runFor:yearly|daily|update>")

  // Get the run variables based on input arguments
  val settings: Settings = Settings.loadFile(Settings.getConfigFile)
  val runConfig = RunConfig.fromArgs(args)

  logger.info(s"Date: ${runConfig.date}, month:${runConfig.month}, year:${runConfig.year}, natco:${runConfig.natco}, " +
      s"natcoNetwork: ${runConfig.natcoNetwork}, runMode:${runConfig.runMode} ")

  // Get settings and create spark session
  implicit val sparkSession: SparkSession = getSparkSession(settings.appName.get)
  val inputDataProvider = new InputDataProvider(settings, runConfig)
  val persitentDataProvider = new PersistentDataProvider(settings, runConfig)

  logger.info("Input files loaded")

  val archiveActivity = Stage.preprocessAccumulator(persitentDataProvider.getActivityArchives)
  val archiveProvision = Stage.preprocessAccumulator(persitentDataProvider.getProvisionArchives)
  val archiveRegisterRequests = Stage.preprocessAccumulator(persitentDataProvider.getRegisterRequestArchives)

  val accActivity = Stage.accumulateActivity(inputDataProvider.getActivityDf, archiveActivity, runConfig)
  val accProvision = Stage.accumulateProvision(inputDataProvider.getProvisionFiles, archiveProvision, runConfig)
  val accRegisterRequests = Stage.accumulateRegisterRequests(inputDataProvider.getRegisterRequests,
    archiveRegisterRequests, runConfig)

  val newUserAgents = DimensionProcessing.getNewUserAgents(inputDataProvider.getActivityDf, inputDataProvider.getRegisterRequests)
  val fullUserAgents = DimensionProcessing.processUserAgentsSCD(persitentDataProvider.getOldUserAgents, newUserAgents)

  val processing = new ProcessingCore(runConfig)

  val outputPath = settings.outputPath.get
  val resultWriter = new ResultWriter(settings, outputPath)

  // yearly processing only
  runConfig.runMode match {
    case RunMode.YEARLY => {
      val (activeYearly, provisionedYearly, registeredYearly) =
        processing.launchYearlyProcessing(accActivity, accProvision, accRegisterRequests, fullUserAgents)

      val fileSuffix = runConfig.natco + "." + runConfig.year + ".csv"

      Map(
        activeYearly -> "activity_yearly.",
        provisionedYearly -> "provisioned_yearly.",
        registeredYearly -> "registered_yearly."
      ).foreach(x => resultWriter.writeWithFixedEmptyDFs(x._1, x._2 + fileSuffix))
    }
    case RunMode.UPDATE => {
      val (activeDaily, activeMonthly, serviceDaily) = processing.processDailyUpdate(accActivity, fullUserAgents)

      Map(
        activeDaily -> ("activity_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv"),
        activeMonthly -> ("activity_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv"),
        serviceDaily -> ("service_fact." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")
      ).foreach(x => resultWriter.writeWithFixedEmptyDFs(x._1, x._2))
    }
    case RunMode.DAILY => {
      val (activeDaily, activeMonthly, serviceDaily) = processing.processDailyUpdate(accActivity, fullUserAgents)
      val (provisionedDaily, registeredDaily, provisionedMonthly, registeredMonthly) =
        processing.processFullDaily(accProvision, accRegisterRequests, fullUserAgents)

      Map(
        activeDaily -> ("activity_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv"),
        activeMonthly -> ("activity_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv"),
        serviceDaily -> ("service_fact." + runConfig.natco + "." + runConfig.dateforoutput + ".csv"),
        provisionedDaily -> ("provisioned_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv"),
        registeredDaily -> ("registered_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv"),
        provisionedMonthly -> ("provisioned_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv"),
        registeredMonthly -> ("registered_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv")
      ).foreach(x => resultWriter.writeWithFixedEmptyDFs(x._1, x._2))
    }
    case RunMode.EOY => {
      val (activeDaily, activeMonthly, serviceDaily) = processing.processDailyUpdate(accActivity, fullUserAgents)
      val activeYearly = processing.endOfYearProcessing(accActivity, fullUserAgents)

      Map(
        activeDaily -> ("activity_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv"),
        activeMonthly -> ("activity_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv"),
        serviceDaily -> ("service_fact." + runConfig.natco + "." + runConfig.dateforoutput + ".csv"),
        activeYearly -> ("activity_yearly." + runConfig.natco + "." + runConfig.year + ".csv")
      ).foreach(x => resultWriter.writeWithFixedEmptyDFs(x._1, x._2))
    }
  }

  val persistencePath = settings.lookupPath.get
  CSVWriter(fullUserAgents, persistencePath + "User_agents.csv", delimiter = "\t").writeData()
}
