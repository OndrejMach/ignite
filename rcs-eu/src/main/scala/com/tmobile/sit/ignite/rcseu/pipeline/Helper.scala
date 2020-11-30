package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.Settings
import org.apache.spark.sql.SparkSession


trait Help extends Logger{
  def resolvePath(settings: Settings, isHistoric: Boolean): String
}

class Helper() (implicit sparkSession: SparkSession) extends Help {

  override def resolvePath(settings: Settings, isHistoric: Boolean): String = {
    // if historic, file is found in the archive
    // check if it exists
    // usually fail if it doesn't except for known list or when isHistoric = true (fast fix)
    // TODO: implement known list
    val path = if(isHistoric) { settings.archivePath.get} else { settings.inputPath.get }
    path
  }
}