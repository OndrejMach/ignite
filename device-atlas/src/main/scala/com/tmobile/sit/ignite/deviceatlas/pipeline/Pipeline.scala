package com.tmobile.sit.ignite.deviceatlas.pipeline

import com.tmobile.sit.ignite.deviceatlas.config.Settings
import com.tmobile.sit.ignite.deviceatlas.data.{InputData, LookupData}

class Pipeline(inputData: InputData, lookupData: LookupData,
               core: ProcessingCore, writer: ResultWriter, settings: Settings, ODATE: String) {
  def run(): Unit = {

    // Run processing core and retrieve result
    val result = core.process(inputData, lookupData, settings, ODATE)

    // Write result data set
    writer.write(result, ODATE)
  }

}
