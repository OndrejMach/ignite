package com.tmobile.sit.ignite.deviceatlas.writers

import java.sql.Date
import org.apache.spark.sql.{DataFrame, SparkSession}

class StageWriter(processingDate: Date, stageData: DataFrame, path: String, partitioned: Boolean = false )
                 (implicit sparkSession: SparkSession) extends ParquetWriter(processingDate = processingDate) {

  def writeData(): Unit = {
    logger.info(s"Writing  data to ${path}, partitioned ${partitioned}")
    writeParquet(stageData, path, partitioned)
    }

  }


