package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame

class SessionsQProcessor(dataCDRsActual: DataFrame, dataCDRsminus1Day: DataFrame, dataCDRsplus1Day: DataFrame) extends Logger {

  val getData: DataFrame = {

  }

}
