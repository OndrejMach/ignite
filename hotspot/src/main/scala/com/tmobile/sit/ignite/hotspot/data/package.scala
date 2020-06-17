package com.tmobile.sit.ignite.hotspot

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * couple of constants which are here basically only to imitate EVL behaviour. Load date and Entry ID should be both removed during the concession clearing, FUTURE may be put into the configuration file - it ws placed here because of
 * security reasons - some logic is treating this as a important value, that's why hardcoded.
 */

package object data {
  val LOAD_DATE = Timestamp.valueOf(LocalDateTime.now())
  val FUTURE = Timestamp.valueOf("4712-12-31 00:00:00")
  val ENTRY_ID = 1
}
