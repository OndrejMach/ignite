package com.tmobile.sit.ignite.rcse.processors.udfs

import java.sql.Date
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import sys.process._
import com.tmobile.sit.ignite.rcse.processors.DatesCount


object UDFs {
  val encode = (encoderPath: String,data: String) => {
    s"${encoderPath} ${data}" !!
  }

  val dateUDF = (date_id: Date,date_queue: Seq[Int], user_queue: Seq[Int], dateUpperBound: Date, cnt_users_all: Int ) => {
    var dateId = date_id.toLocalDate
    var dQueue = date_queue; var uQueue = user_queue
    var cntUsersAll = cnt_users_all
    var ret = ListBuffer[DatesCount]()
    val refDateLocalTime = LocalDate.of(1900,1,1)
    while (dateId.isBefore(dateUpperBound.toLocalDate) || (dateId.isEqual(dateUpperBound.toLocalDate))) {
      var rcse_reg_users_new = 0
      if (!dQueue.isEmpty && dQueue.head == DAYS.between(dateId, refDateLocalTime)) {
        rcse_reg_users_new = uQueue.head
        cntUsersAll += uQueue.head
        dQueue = dQueue.tail; uQueue = uQueue.tail
      }
      ret += DatesCount(Date.valueOf(dateId),rcse_reg_users_new, cntUsersAll )
      dateId = dateId.plusDays(1)
    }
    ret
  }

}
