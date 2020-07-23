package com.tmobile.sit.ignite.common

package object processing {
  def translateSeconds = (secs: Long) => {
    def pad(n: Long): String = {
      if (n < 10) "0" + n.toString else n.toString
    }
    if (secs <= 0) {
      "00:00:00"
    } else {
      val hours = secs / 3600
      val minutes = secs % 3600 / 60
      val seconds = (secs % 3600) % 60
      s"${pad(hours)}:${pad(minutes)}:${pad(seconds)}"
    }
  }
  def translateHours = (hrs: Long) => {
    def pad(n: Long): String = {
      if (n < 10) "0" + n.toString else n.toString
    }
    if (hrs <= 0) {
      "00:00:00"
    } else {
      val days = hrs / 24
      val hours = hrs - (days * 24)
      val minutes = (hrs - (days * 24)) / 60
      s"${pad(days)}:${pad(hours)}:${pad(minutes)}"
    }
  }

}
