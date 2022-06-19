package com.tmobile.sit.ignite.rcseu.config

import com.tmobile.sit.ignite.rcseu.RunMode
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class RunConfig(date: LocalDate, natco: String, runFor: String,
                     year: String, monthNum: String, dayNum: String,
                     debug: Boolean = false) {

  val processYearly: Boolean = if (runFor.equals("yearly")) true
  else false

  val month: String = year + "-" + monthNum
  val monthforkey: String = year + "\\" + monthNum
  val dayforkey: String = dayNum + "-" + monthNum + "-" + year

  val dateforoutput: String = year + monthNum + dayNum
  val monthforoutput: String = year + monthNum

  val natcoID: String = RunConfig.natcoIdMap.getOrElse(natco, "natco ID is not correct")
  val natcoNetwork: String = RunConfig.natcoNetworkMap.getOrElse(natco, "natco network is not correct")
  val tomorrowDate: String = date.plusDays(1).toString

  val archiveFilter: Column = // if yearly reprocessing or update on 31st of January
    if (runFor.equals("yearly") || (runFor.equals("update") && date.toString.endsWith("-12-31"))) {
    org.apache.spark.sql.functions.year(col("date")) === year.toInt
  } else {
    (org.apache.spark.sql.functions.year(col("date")) === year.toInt) &&
      (org.apache.spark.sql.functions.month(col("date")) === monthNum.toInt)
  }

  val runMode = if (processYearly) {
    RunMode.YEARLY
  } else {
    if (runFor.equals("update")) {
      RunMode.UPDATE
    } else if (date.format(DateTimeFormatter.ISO_DATE).endsWith("-12-31")) {
      RunMode.EOY
    } else RunMode.DAILY
  }
}

object RunConfig {
  val natcoIdMap = Map(
    "mt" -> "1",
    "cg" -> "2",
    "st" -> "3",
    "cr" -> "4",
    "tc" -> "6",
    "mk" -> "7",
    "tp" -> "8"
  )

  val natcoNetworkMap = Map(
    "st" -> "dt-slovak-telecom",
    "mt" -> "dt-magyar-telecom",
    "cg" -> "dt-cosmote-greece",
    "cr" -> "dt-telecom-romania",
    "mk" -> "dt-makedonski-telecom",
    "tp" -> "tmobile-polska-poland",
    "tc" -> "dt-tmobile-czech-republic"
  )

  def fromArgs(args: Array[String]): RunConfig = {
    val dateArg = args(0)
    val natco = args(1)
    val runFor = args(2)

    val date = LocalDate.parse(dateArg, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val date_split = dateArg.split('-')
    val (year, monthNum, dayNum) = (date_split(0), date_split(1), date_split(2))

    RunConfig(date, natco, runFor, year, monthNum, dayNum)
  }
}
