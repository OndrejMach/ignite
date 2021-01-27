package com.tmobile.sit.ignite.inflight.processing.writers

import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
 * Types representing Excell reports kinds - Daily or Monthly. Both have to provide relevant filenames and also name of the report (Daily, Monthly)
 */

trait ExcelReportType {
  def getFilenameSession(airlineCode: String, date: Timestamp): String
  def getFilenameVoucher(airlineCode: String, date: Timestamp): String
  def getName: String
}

class DailySessionReport extends ExcelReportType {

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

    val d = new SimpleDateFormat("yyyy_MM").format(date)
    s"${airlineCode}_avg._MB_usage_${d}.xlsx"
  }
  override def getFilenameVoucher(airlineCode: String, date: Timestamp): String = {
    val d = new SimpleDateFormat("yyyy_MM").format(date)
    s"${airlineCode}_detailed_MB_usage_${d}.xlsx"
  }

  override def getName: String = "Monthly"
}
