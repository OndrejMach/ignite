package com.tmobile.sit.ignite.deviceatlas.writers

import org.apache.spark.sql.{DataFrame, SparkSession}

class StageWriter(stageData: DataFrame, path: String, partitioned: Boolean = false )
                 (implicit sparkSession: SparkSession) extends ParquetWriter {

  def writeData(): Unit = {
    logger.info(s"Writing  data to ${path}, partitioned ${partitioned}")
    writeParquet(stageData, path, partitioned)
    }

  }


