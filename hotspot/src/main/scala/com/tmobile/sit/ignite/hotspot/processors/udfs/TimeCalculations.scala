package com.tmobile.sit.ignite.hotspot.processors.udfs

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import java.time.temporal.ChronoUnit
import sys.process._
import com.tmobile.sit.ignite.hotspot.processors.SessionMetrics

object DirtyStuff{
  val encode = (encoderPath: String,data: String) => {
    s"${encoderPath} ${data}" !!
  }

}


object TimeCalculations {
  val toQuarters = (a: Timestamp, e: Timestamp, volumeRatio: Double) => {
    val endDate = if (e.toLocalDateTime.getDayOfMonth != a.toLocalDateTime.getDayOfMonth) {
      val plus1Day = a.toLocalDateTime.plusDays(1)
      LocalDateTime.of(plus1Day.getYear, plus1Day.getMonth, plus1Day.getDayOfMonth, 0, 0, 0, 0)
    } else {
      e.toLocalDateTime
    }

    def getNextQuarter(d: LocalDateTime): LocalDateTime = {
      val nextQuarter = d.plusMinutes(15 - (d.getMinute % 15))
      LocalDateTime.of(nextQuarter.getYear, nextQuarter.getMonth, nextQuarter.getDayOfMonth, nextQuarter.getHour, nextQuarter.getMinute, 0, 0)
    }

    def getQuarterID(d: LocalDateTime): Long = {
      val zoneId = ZoneId.systemDefault() // or: ZoneId.of("Europe/Oslo");
      val secs = d.atZone(zoneId).toEpochSecond()
      (secs - (secs % 900))%86400/60
    }

    def durationBetween(a: LocalDateTime) : (Long, Int) = {
      //println(s"a: ${a.getYear}-${a.getMonth}-${a.getDayOfMonth} e:${endDate.getYear}-${endDate.getMonth}-${endDate.getDayOfMonth}")
      if (((a.getMinute / 15) == (endDate.getMinute / 15)) && (ChronoUnit.MINUTES.between(a, endDate) <= 15)) (ChronoUnit.SECONDS.between(a, endDate),1)
      else {
        val next = getNextQuarter(a)
        (ChronoUnit.MINUTES.between(a, next)*60, 0)
      }
    }

    for {i <- 0 to (ChronoUnit.MINUTES.between(a.toLocalDateTime, endDate).toInt / 15)} yield {
      if (i == 0) {
        val (duration, endFlag) = durationBetween(a.toLocalDateTime)
        SessionMetrics(
          quarterID = getQuarterID(a.toLocalDateTime),
          duration = duration,
          volume = volumeRatio * duration,
          start_flag = 1,
          end_flag = endFlag
        )
      } else {
        val (duration, endFlag) = durationBetween(getNextQuarter(a.toLocalDateTime.plusMinutes((i - 1) * 15)))
        SessionMetrics(
          quarterID = getQuarterID(a.toLocalDateTime.plusMinutes(i * 15)),
          duration = duration,
          volume = volumeRatio * duration,
          end_flag = endFlag,
          start_flag = 0
        )
      }
    }
  }
}
