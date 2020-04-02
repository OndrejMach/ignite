package com.tmobile.sit.ignite.inflight.processing.writers

import java.sql.Timestamp
import java.text.SimpleDateFormat

trait ExcelReportType {
  def getFilenameSession(airlineCode: String, date: Timestamp): String
  def getFilenameVoucher(airlineCode: String, date: Timestamp): String
  def getName: String
}

class DailySessionReport extends ExcelReportType {
  //${AIRLINE}_avg._MB_usage_${YEAR}_${MONTH}_${DAY}.xlsx
  override def getFilenameSession(airlineCode: String, date: Timestamp): String = {
    val d = new SimpleDateFormat("yyyy_MM_dd").format(date)
    s"${airlineCode}_avg._MB_usage_${d}.xlsx"
  }

  override def getFilenameVoucher(airlineCode: String, date: Timestamp): String = {
    val d= new SimpleDateFormat("yyyy_MM_dd").format(date)
    s"${airlineCode}_detailed_MB_usage_${d}.xlsx"
  }
  override def getName: String = "Daily"
}

class MonthlySessionReport extends ExcelReportType {
  override def getFilenameSession(airlineCode: String, date: Timestamp): String = {
    //${AIRLINE}_avg._MB_usage_${ODATE_MINUS1MONTH:0:4}_${ODATE_MINUS1MONTH:4:2}.xlsx
    val d = new SimpleDateFormat("yyyy_MM").format(date)
    s"${airlineCode}_avg._MB_usage_${d}.xlsx"
  }
  override def getFilenameVoucher(airlineCode: String, date: Timestamp): String = {
    val d = new SimpleDateFormat("yyyy_MM").format(date)
    s"${airlineCode}_detailed_MB_usage_${d}.xlsx"
  }

  override def getName: String = "Monthly"
}
