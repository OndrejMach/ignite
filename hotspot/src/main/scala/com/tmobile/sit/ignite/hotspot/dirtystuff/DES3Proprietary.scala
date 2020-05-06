package com.tmobile.sit.ignite.hotspot.dirtystuff
import sys.process._


object DES3Proprietary {
  def encrypt(encoderBinaryPath: String, data: String) : String = {
    s"${encoderBinaryPath} ${data}" !!
  }
}
