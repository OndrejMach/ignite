package com.tmobile.sit.ignite.rcseu.config

import com.tmobile.sit.common.config.GenericSettings


case class Settings(inputPath: Option[String]
                    , outputPath: Option[String]
                    , lookupPath: Option[String]
                    , appName: Option[String]
                   ) extends GenericSettings
{

  def isAllDefined: Boolean = {
    this.inputPath.isDefined && this.inputPath.get.nonEmpty &&
     this.lookupPath.isDefined && this.outputPath.get.nonEmpty &&
      this.outputPath.isDefined && this.outputPath.get.nonEmpty &&
      this.appName.isDefined && this.appName.get.nonEmpty
  }
}
