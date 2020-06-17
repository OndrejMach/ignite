package com.tmobile.sit.ignite.rcseu.config

import com.tmobile.sit.common.config.ServiceConfig


class Setup(configFile: String = "job_template.conf")  {

  val settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      appName = Option(serviceConf.envOrElseConfig("configuration.appName.value"))
      , inputPath = Option(serviceConf.envOrElseConfig("configuration.inputPath.value"))
      , outputPath = Option(serviceConf.envOrElseConfig("configuration.outputPath.value")
      ))
  }
}
