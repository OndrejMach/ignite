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
  val precisionCorrection = (number: Double) => {
    val arr = number.toString.split("\\.")
    val prec = if (arr(1).length == 1) {
      arr(1)+"0"
    } else {
      arr(1)
    }
    s"${arr(0)}.${prec}"
  }
  val padPercentage = (s: String) => {
    s.replaceAll("\\.0%", "\\.00%")
  }

  val removeTrailing0s = (l: Double) => {
    val arr = l.toString.split("\\.")
    if (arr(1) == "0") {
      arr(0)
    } else {
      l.toString
    }
  }
}




object TimeCalculations {
  val toQuarters = (a: Timestamp, e: Timestamp, volume: Long, duration: Long, processingDate: Timestamp) => {
    val volumeRatio = if (duration == 0) 0.0 else volume.toDouble/duration.toDouble
    val startTime = if (a.before(processingDate)) processingDate else a
    val startFlag = if (a.before(processingDate)) 0 else 1
    val endDate = //if (e.toLocalDateTime.getDayOfMonth != a.toLocalDateTime.getDayOfMonth) {
      //val plus1Day = a.toLocalDateTime.plusDays(1)
     // LocalDateTime.of(plus1Day.getYear, plus1Day.getMonth, plus1Day.getDayOfMonth, 0, 0, 0, 0)
   // } else {
      e.toLocalDateTime
   // }

    def getNextQuarter(d: LocalDateTime): LocalDateTime = {
      val nextQuarter = d.plusMinutes(15 - (d.getMinute % 15))
      LocalDateTime.of(nextQuarter.getYear, nextQuarter.getMonth, nextQuarter.getDayOfMonth, nextQuarter.getHour, nextQuarter.getMinute, 0, 0)
    }

    def getQuarterID(d: LocalDateTime): Long = {
     // val zoneId = ZoneId.of("UTC") //ZoneId.systemDefault() // or: ZoneId.of("Europe/Oslo");
     // val secs = d.atZone(zoneId).toEpochSecond()
     val secs = d.getHour*3600 + d.getMinute*60 + d.getSecond
      (secs/900)*15
      //((secs - (secs % 900))%86400)/60
    }

    def durationBetween(a: LocalDateTime) : (Long, Int) = {
      //println(s"a: ${a.getYear}-${a.getMonth}-${a.getDayOfMonth} e:${endDate.getYear}-${endDate.getMonth}-${endDate.getDayOfMonth}")
      if (((a.getMinute / 15) == (endDate.getMinute / 15)) && (ChronoUnit.MINUTES.between(a, endDate) <= 15)){
        (ChronoUnit.SECONDS.between(a, endDate),1)
      }
      else {
        val next = getNextQuarter(a)
        (ChronoUnit.SECONDS.between(a, next), 0)
      }
    }

    for {i <- 0 to (ChronoUnit.MINUTES.between(startTime.toLocalDateTime, endDate).toInt / 15)} yield {
      if (i == 0) {
        val (durationCalculated, endFlag) = durationBetween(startTime.toLocalDateTime)
        SessionMetrics(
          quarterID = getQuarterID(startTime.toLocalDateTime),
          duration = if ((endFlag==1) && (startFlag==1)) duration else durationCalculated,
          volume = if ((endFlag==1) && (startFlag==1)) volume else volumeRatio * durationCalculated,
          start_flag = startFlag,
          end_flag = endFlag
        )
      } else {
        val (durationCalculated, endFlag) = durationBetween(getNextQuarter(startTime.toLocalDateTime.plusMinutes((i - 1) * 15)))
        SessionMetrics(
          quarterID = getQuarterID(startTime.toLocalDateTime.plusMinutes(i * 15)),
          duration = durationCalculated,
          volume = volumeRatio * durationCalculated,
          end_flag = endFlag,
          start_flag = 0
        )
      }
    }
  }
}
