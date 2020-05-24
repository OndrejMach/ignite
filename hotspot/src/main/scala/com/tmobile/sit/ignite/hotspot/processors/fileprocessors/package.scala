package com.tmobile.sit.ignite.hotspot.processors

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
}
