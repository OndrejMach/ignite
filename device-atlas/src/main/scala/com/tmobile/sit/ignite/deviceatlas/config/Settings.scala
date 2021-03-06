package com.tmobile.sit.ignite.deviceatlas.config

import com.tmobile.sit.ignite.common.common.config.GenericSettings

case class Settings(inputPath: Option[String]
                    , lookupPath: Option[String]
                    , outputPath: Option[String]
                    , workPath: Option [String]
                    , stagePath: Option [String]
                    , appName: Option[String]
                   ) extends GenericSettings
{

  def isAllDefined: Boolean = {
    this.inputPath.isDefined && this.inputPath.get.nonEmpty &&
      this.lookupPath.isDefined && this.lookupPath.get.nonEmpty &&
      this.outputPath.isDefined && this.outputPath.get.nonEmpty &&
      this.workPath.isDefined && this.workPath.get.nonEmpty &&
      this.stagePath.isDefined && this.stagePath.get.nonEmpty &&
      this.appName.isDefined && this.appName.get.nonEmpty
  }
}
