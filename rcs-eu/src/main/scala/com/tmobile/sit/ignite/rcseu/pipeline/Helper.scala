package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.{date, natco}
import com.tmobile.sit.ignite.rcseu.config.Settings
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


trait Help extends Logger{
  def resolvePath(settings:Settings,date:String,natco:String,isHistoric:Boolean,fileName:String):String
}

class Helper() (implicit sparkSession: SparkSession) extends Help {

  override def resolvePath(settings:Settings,date:String,natco:String,isHistoric:Boolean,fileName:String):String = {
    // if historic, file is found in the archive
    // check if it exists
    // usually fail if it doesn't except for known list or when isHistoric = true (fast fix)
    // TODO: implement known list
    val path = if(isHistoric) { settings.archivePath.get} else { settings.inputPath.get }
    // expected file like /data/sit/rcseu/input|archive/activity_2020-01-01_mt.csv.gz
    val expectedFile = s"${path}${fileName}_${date}.csv_${natco}.csv.gz"
    // if file doesn't exist, use dummy one
    val dummyFile = s"${settings.lookupPath.get}empty_${fileName}.csv"

    //check for file and if historic
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(expectedFile))

    var result = new String

    if(fileExists) {
      logger.info(s"File $expectedFile exists: $fileExists")
      result = expectedFile
    } else { //file doesn't exist
      logger.warn(s"File $expectedFile exists: $fileExists")
      if(isHistoric) { // If during a historic reprocessing run, replace with dummy file and move on
        logger.warn(s"Using dummy file: $dummyFile")
        result = dummyFile
      }
      else {
        logger.error(s"File missing during non-historic run: $expectedFile")
        System.exit(1)
      }
    }
    // Return correct result
    result
  }
}