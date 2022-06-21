package com.tmobile.sit.ignite.rcseu.config

import com.tmobile.sit.ignite.common.common.config.GenericSettings


case class Settings(inputPath: Option[String]
                    , outputPath: Option[String]
                    , lookupPath: Option[String]
                    , archivePath: Option[String]
                    , appName: Option[String]
                   ) extends GenericSettings
{

  def isAllDefined: Boolean = {
    this.inputPath.isDefined && this.inputPath.get.nonEmpty &&
     this.lookupPath.isDefined && this.lookupPath.get.nonEmpty &&
      this.outputPath.isDefined && this.outputPath.get.nonEmpty &&
      this.archivePath.isDefined && this.archivePath.get.nonEmpty &&
      this.appName.isDefined && this.appName.get.nonEmpty
  }
}
