package com.tmobile.sit.ignite.hotspot.processors

import java.time.{LocalDateTime, ZoneId}

/**
 * some helper methods for mostly data transformation
 */

package object fileprocessors {
  def getString(s: String): Option[String] = if (s.isEmpty) None else Some(s)

  def getDouble(s: String): Option[Double] = if (s.isEmpty) None else {
    try {
      Some(s.toDouble)
    } catch {
      case e: Exception => None
    }
  }

  def getLong(s: String): Option[Long] = if (s.isEmpty) None else {
    try {
      Some(s.toLong)
    } catch {
      case e: Exception => None
    }
  }

  val getTimeZoneOffset : Long = {
    val utc = ZoneId.of("UTC")
    val ldt = LocalDateTime.now()
    val utcDT = LocalDateTime.now(utc)

    import java.time.temporal._

    Math.round(utcDT.until(ldt, ChronoUnit.SECONDS).toDouble/3600)
  }
}
