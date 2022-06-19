package com.tmobile.sit.ignite.rcseu

import java.time.format.DateTimeFormatter

package object data {
  val dateFormatPattern = "yyyy-MM-dd"
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateFormatPattern)

  val dataTimeZone = "UTC"
}
