package com.tmobile.sit.ignite.hotspot

import java.sql.Timestamp
import java.time.LocalDateTime

package object data {
  val LOAD_DATE = Timestamp.valueOf(LocalDateTime.now())
  val FUTURE = Timestamp.valueOf("4712-12-31 00:00:00")
  val ENTRY_ID = 1
}
