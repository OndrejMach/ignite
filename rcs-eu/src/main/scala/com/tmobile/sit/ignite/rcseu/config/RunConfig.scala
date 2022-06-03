package com.tmobile.sit.ignite.rcseu.config

import com.tmobile.sit.ignite.rcseu.RunMode

import java.time.LocalDate
import java.time.format.DateTimeFormatter

// variables needed in FactsProcesing and ProcessingCore for filtering
//case class RunConfig(_args: Array[String]) {
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
  val tomorrowDate: String = date.plusDays(1).toString()

  val archiveFileMask: String = {
    // if yearly reprocessing or update on 31st of January
    if (runFor.equals("yearly") || (runFor.equals("update") && date.toString.endsWith("-12-31"))) year
    else month
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
    "tp" -> "8",
    "td" -> "9"
  )

  val natcoNetworkMap = Map(
    "st" -> "dt-slovak-telecom",
    "mt" -> "dt-magyar-telecom",
    "cg" -> "dt-cosmote-greece",
    "cr" -> "dt-telecom-romania",
    "mk" -> "dt-makedonski-telecom",
    "tp" -> "tmobile-polska-poland",
    "tc" -> "dt-tmobile-czech-republic",
    "td" -> "Telekom Deutschland"
  )

  def fromArgs(args: Array[String]): RunConfig = {

    val dateArg = args(0)
    val natco = args(1)
    val runFor = args(2)


    val date = LocalDate.parse(dateArg, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    //    val tomorrowDate = date.plusDays(1).toString()

    //    val debug = false;

    //    val processYearly = if (runMode.equals("yearly")) {
    //      true
    //    } else {
    //      false
    //    }

    val date_split = dateArg.split('-')
    val (year, monthNum, dayNum) = (date_split(0), date_split(1), date_split(2))
    //    val month = year + "-" + monthNum

    //    val monthforkey = year + "\\" + monthNum
    //    val dayforkey = dayNum + "-" + monthNum + "-" + year
    //
    //    val dateforoutput = year + monthNum + dayNum
    //    val monthforoutput = year + monthNum

    //    val mtID = "1"
    //    val stID = "3"
    //    val cgID = "2"
    //    val crID = "4"
    //    val mkID = "7"
    //    val tpID = "8"
    //    val tcID = "6"
    //
    //    val natcoID = if (natco == "mt") mtID
    //    else if (natco == "st") stID
    //    else if (natco == "cr") crID
    //    else if (natco == "cg") cgID
    //    else if (natco == "mk") mkID
    //    else if (natco == "tp") tpID
    //    else if (natco == "tc") tcID
    //    else "natco ID is not correct"

    //    val mt = "dt-magyar-telecom"
    //    val st = "dt-slovak-telecom"
    //    val cg = "dt-cosmote-greece"
    //    val cr = "dt-telecom-romania"
    //    val mk = "dt-makedonski-telecom"
    //    val tp = "tmobile-polska-poland"
    //    val tc = "dt-tmobile-czech-republic"
    //
    //    val natcoNetwork = if (natco == "mt") mt
    //    else if (natco == "st") st
    //    else if (natco == "cr") cr
    //    else if (natco == "cg") cg
    //    else if (natco == "mk") mk
    //    else if (natco == "tp") tp
    //    else if (natco == "tc") tc
    //    else "natco network is not correct"
    RunConfig(date, natco, runFor, year, monthNum, dayNum)
  }
}
