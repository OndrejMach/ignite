package com.tmobile.sit.ignite.rcseu.config
import java.time.LocalDate
import java.time.format.DateTimeFormatter
// variables needed in FactsProcesing and ProcessingCore for filtering
class RunConfig(_args: Array[String]) {
  val args = _args

  val date = args(0)
  val natco = args(1)
  val runMode = args(2)


  val dateformat = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val tomorrowDate=dateformat.plusDays(1).toString()

  val debug = false;
  val processDaily = true;
  val processMonthly = true;

  val processYearly = if(runMode.equals("yearly")) {true} else {false}

  val date_split = date.split('-')
  val (year, monthNum, dayNum) = (date_split(0), date_split(1),date_split(2))
  val month = year + "-" + monthNum

  val monthforkey = year + "\\" +monthNum
  val dayforkey =  dayNum +"-"+ monthNum +"-"+ year

  val dateforoutput = year+monthNum+dayNum
  val monthforoutput = year+monthNum

  val mtID="1"
  val stID="3"
  val cgID="2"
  val crID="4"
  val mkID="7"

  val natcoID = if (natco == "mt") mtID
  else if (natco == "st") stID
  else if (natco == "cr") crID
  else if (natco == "cg") cgID
  else if (natco == "mk") mkID
  else "natco ID is not correct"

  val mt="dt-magyar-telecom"
  val st="dt-slovak-telecom"
  val cg="dt-cosmote-greece"
  val cr="dt-telecom-romania"
  val mk="dt-makedonski-telecom"

  val natcoNetwork = if (natco == "mt") mt
  else if (natco == "st") st
  else if (natco == "cr") cr
  else if (natco == "cg") cg
  else if (natco == "mk") mk
  else "natco network is not correct"
}
