package com.tmobile.sit.ignite.jobtemplate.config

import com.tmobile.sit.ignite.common.config.ServiceConfig

class Setup(configFile: String = "job_template.conf")  {

  val settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      appName = Option(serviceConf.envOrElseConfig("configuration.appName.value"))
      , inputPathPeople = Option(serviceConf.envOrElseConfig("configuration.inputPathPeople.value"))
      , inputPathSalaryInfo = Option(serviceConf.envOrElseConfig("configuration.inputPathSalaryInfo.value"))
      , outputPath = Option(serviceConf.envOrElseConfig("configuration.outputPath.value"))
     )
  }
}
