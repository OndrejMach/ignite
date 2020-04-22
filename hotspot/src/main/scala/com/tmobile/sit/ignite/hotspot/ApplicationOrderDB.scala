package com.tmobile.sit.ignite.hotspot

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType


case class OrderDBInput(
                         data_code: Option[String],
                         transaction_id:Option[String],
                         transaction_date: Option[String],
                         transaction_time: Option[String],
                         paytid: Option[String],
                         error_code: Option[String],
                         email: Option[String],
                         amount: Option[String],
                         currency: Option[String],
                         result_code: Option[String],
                         cancellation: Option[String],
                         card_institute: Option[String],
                         vat: Option[String],
                         payment_method: Option[String],
                         voucher_type: Option[String],
                         hotspot_name: Option[String],
                         natco: Option[String],
                         username: Option[String],
                         ma_name: Option[String],
                         voucher_duration: Option[Long],
                         number_miles: Option[Double],
                         alternate_currency: Option[String],
                         reduced_amount: Option[Double],
                         campaign_name: Option[String]
                       //  rest: Option[String]
                       )

case class ErrorCode(
                    error_code: Option[String],
                    error_message: Option[String],
                    error_desc: Option[String],
                    valid_from: Option[Timestamp],
                    valid_to: Option[Timestamp],
                    entry_id: Option[Long],
                    load_date: Option[Timestamp]
                    )


object ApplicationOrderDB  {
  val LOAD_DATE = Timestamp.valueOf(LocalDateTime.now())
  val FUTURE = Timestamp.valueOf("4712-12-31 00:00:00")
  val ENTRY_ID = 1



  def getWlanHostpotIDsSorted(data: DataFrame) : DataFrame = {
    data
      .select(col("_c0").cast(LongType))
      .sort(desc("_c0"))
      .distinct()
  }

  def mapInputOrderDB(line: String): OrderDBInput = {
    def getString(s: String) : Option[String] = if (s.isEmpty) None else Some(s)
    def getDouble(s: String) : Option[Double] = if (s.isEmpty) None else { try { Some(s.toDouble)} catch {case e: Exception => None}}
    def getLong(s: String) : Option[Long] = if (s.isEmpty) None else { try { Some(s.toLong)} catch {case e: Exception => None}}
    val values = line.split(";", -1)
    OrderDBInput(
      data_code=getString(values(0)),
      transaction_id =getString(values(1)),
      transaction_date = getString(values(2)),
      transaction_time = getString(values(3)),
      paytid = getString(values(4)),
      error_code = getString(values(5)),
      email =getString(values(6)),
      amount = getString(values(7)),
      currency = getString(values(8)),
      result_code = getString(values(9)),
      cancellation = getString(values(10)),
      card_institute = getString(values(11)),
      vat = getString(values(12)),
      payment_method = getString(values(13)),
      voucher_type = getString(values(14)),
      hotspot_name = getString(values(15)),
      natco = getString(values(16)),
      username = getString(values(17)),
      ma_name = getString(values(18)),
      voucher_duration = getLong(values(19)),
      number_miles = getDouble(values(20)),
      alternate_currency = getString(values(21)),
      reduced_amount = getDouble(values(22)),
      campaign_name = getString(values(23))
    )
  }

  def mapErrorCodes(entry: OrderDBInput) :ErrorCode = {
    ErrorCode(
      error_code = if (entry.error_code.isDefined) Some(entry.error_code.get.toUpperCase) else None,
      error_message = entry.error_code,
      error_desc = Some("UNKNOWN"),
      valid_from = Some(LOAD_DATE),
      valid_to = Some(FUTURE),
      entry_id = Some(ENTRY_ID),
      load_date = Some(LOAD_DATE)
    )
  }


  def main(args: Array[String]): Unit = {
    val sparkSession = getSparkSession()
    import sparkSession.implicits._
    val dataHotspotIDs = sparkSession
     .read
    .option("delimiter", "~")
    .csv("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/cptm_ta_d_wlan_hotspot.csv")

    println(getWlanHostpotIDsSorted(dataHotspotIDs).first().getLong(0))

    val inputMPS = sparkSession
     .read
     .text("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.MPS.DAY.20200408*.csv")
    inputMPS.printSchema()

    println("D;79746013287330544778;20200407;090724;;MPSI;;4.95;EUR;KO;;MOBILE_APERTO;19.0;mobile_aperto;PASS;DE/TCOM/ForFreePass_30min_Venue/DB_Bahnhoefe/Freiburg_Brsg./761138399700/XYZ;TMD;;lukasmax57@yahoo.com;86400;;;;;;;;;;;".split(";", -1).size)

    val inputFiltered = inputMPS
      .filter(col("value").startsWith("D;"))
      .as[String]
        .map(mapInputOrderDB(_))

    val errorCodes = inputFiltered.map(mapErrorCodes(_)).distinct()
    errorCodes.show(false)

    inputFiltered.show(false)

    //TBL_NAME=orderdb
    //TBL_PREF=cptm_ta_f_wlan

   // "D;79746013287330544778;20200407;090724;;MPSI;;4.95;EUR;KO;;MOBILE_APERTO;19.0;mobile_aperto;PASS;DE/TCOM/ForFreePass_30min_Venue/DB_Bahnhoefe/Freiburg_Brsg./761138399700/XYZ;TMD;;lukasmax57@yahoo.com;86400;;;;;;;;;;;".split(";").foreach(println(_))


  }


}
