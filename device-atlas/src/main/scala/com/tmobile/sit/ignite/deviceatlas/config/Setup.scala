package com.tmobile.sit.ignite.deviceatlas.config

import com.tmobile.sit.common.config.ServiceConfig

class Setup(configFile: String = "device-atlas.conf")  {

  val settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      appName = Option(serviceConf.envOrElseConfig("configuration.appName.value"))
      , inputPath = Option(serviceConf.envOrElseConfig("configuration.inputPath.value"))
      , lookupPath = Option(serviceConf.envOrElseConfig("configuration.lookupPath.value"))
      , outputPath = Option(serviceConf.envOrElseConfig("configuration.outputPath.value"))
      , workPath = Option(serviceConf.envOrElseConfig("configuration.workPath.value"))
      , stagePath = Option(serviceConf.envOrElseConfig("configuration.stagePath.value"))
     )
  }
}
