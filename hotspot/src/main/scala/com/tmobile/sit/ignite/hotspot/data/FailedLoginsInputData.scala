package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.FailedLoginsStructure.FailedLogin
import com.tmobile.sit.ignite.hotspot.readers.TextReader
import org.apache.spark.sql.SparkSession

/**
 * Class wrapping Failed logins input data
 * @param settings
 * @param sparkSession
 */

class FailedLoginsInputData(implicit settings: Settings, sparkSession: SparkSession) extends Logger{
 import sparkSession.implicits._

  lazy val failedLogins = {
    logger.info(s"Reading failed login files ${settings.inputConfig.failed_login_filename.get}")
    val data = new TextReader(path = settings.inputConfig.failed_login_filename.get).read()
    logger.info("Reading failed logins raw data")
    val ret = data
      //.read()
      .filter($"value".startsWith("D;"))
      .as[String]
      .map(i => FailedLogin(i)).toDF()
    logger.info(s"rawDataCount = ${ret.count()}") //0562862139
    ret
  }

  lazy val loginErrorCodes = {
    logger.info("Reading LoginError codes")
    CSVReader(path = settings.stageConfig.login_errors.get,
      header = false, schema = Some(ErrorCodes.loginErrorStruct),
      delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss").read()
  }
}
