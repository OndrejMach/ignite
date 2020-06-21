package com.tmobile.sit.ignite.rcse.processors.udfs

import sys.process._

object UDFs {
  val encode = (encoderPath: String,data: String) => {
    s"${encoderPath} ${data}" !!
  }

}
