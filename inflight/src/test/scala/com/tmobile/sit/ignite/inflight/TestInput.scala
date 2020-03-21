package com.tmobile.sit.ignite.inflight

import java.sql.Timestamp
import java.time.LocalDateTime

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.tmobile.sit.common.readers.{CSVMultifileReader, CSVReader, ExcelReader, MSAccessReader}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import com.tmobile.sit.ignite.inflight.datastructures.{InputStructures, InputTypes}
import com.tmobile.sit.ignite.inflight.processing.StageProcess


@RunWith(classOf[JUnitRunner])
class JobTemplateTest extends FlatSpec with DataFrameSuiteBase {
  implicit lazy val _: SparkSession = spark

  "csvReader" should "read Aircraft file" in {
    import spark.implicits._

    val csvReader = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_aircraft.csv",
      header = false,
      schema = Some(InputStructures.aircraftStruct)
      ,delimiter = "|")

    val df = csvReader.read().as[InputTypes.Aircraft].filter("hotspot_id = 'DE_TW5459'")

    val refDF = ReferenceData.inputAircraft.toDF
    assertDataFrameEquals(df.toDF(), refDF)

  }

  "Input Airline data" should "be preprocessed" in {
    import spark.implicits._

    val csvReader = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_airline.csv",
      header = false,
      schema = Some(InputStructures.airlineStructure)
      ,delimiter = "|")

    val df = csvReader.read().as[InputTypes.Airline]//.filter("airline_name = 'Scoot'")
    val stage = new StageProcess()

    val preprocessed = stage.processAirline(df).filter("wlif_airline_desc = 'Scoot'")

    assertDataFrameEquals(preprocessed.toDF(), ReferenceData.stagedAirline.toDF())
  }

  "Preprocess Aircraft" should "clean Aircraft input" in {
    import spark.implicits._

    val df = ReferenceData.inputAircraft
    val stage = new StageProcess()
    val staged = stage.processAircraft(df.toDS()).toDF()
    val refDF = ReferenceData.stagedAircraft.toDF()

    //staged.printSchema()
    //refDF.printSchema()
    assertDataFrameEquals(staged, refDF)
  }

  "Preprocess Airport" should "clean airport input" in {
    import spark.implicits._

    val csvReader = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_airport.csv",
      header = false,
      schema = Some(InputStructures.airportStructure)
      , delimiter = "|")

    val df = csvReader.read().as[InputTypes.Airport] //.filter("airline_name = 'Scoot'")
    val stage = new StageProcess()

    val preprocessed = stage.processAirport(df).filter("wlif_city = 'GOROKA'")

    assertDataFrameEquals(preprocessed.toDF(), ReferenceData.stagedAirport.toDF())
  }

  "Realm preprocessing" should "clean realm input" in {
    import spark.implicits._
    val csvReader = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_account_type.csv",
      header = false,
      schema = Some(InputStructures.realmStructure)
      , delimiter = "|")

    val df = csvReader.read().as[InputTypes.Realm]
    val stage = new StageProcess()

    val preprocessed = stage.preprocessRealm(df).filter("wlif_realm_desc = 't-online.de'")

    assertDataFrameEquals(preprocessed.toDF(), ReferenceData.stagedRealm.toDF())
  }

  "Oooid preprocessing" should "clean oooid input" in {
    import spark.implicits._

    val csvReader = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_oooi.csv",
      header = false,
      schema = Some(InputStructures.oooidStructure),
       delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )//2020-01-22 02:48:00
    //csvReader.read().printSchema()

    val df = csvReader.read().as[InputTypes.Oooid]

    val stage = new StageProcess()

    val loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now())

    val preprocessed = stage.processOooid(df, loadDate=loadDate).filter("wlif_sequence = '9842350'")

    assertDataFrameEquals(preprocessed.toDF(), ReferenceData.stagedOooi(loadDate).toDF())
  }

  "Radius preprocessing" should "clean Radius input" in {
    import spark.implicits._
    //G_2020-02-12_03-35-07_radius.csv
    val csvReader = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_radius.csv",
      header = false,
      schema = Some(InputStructures.radiusStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )

    val df = csvReader.read()

    val stage = new StageProcess()

    val loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now())
    val radius = stage.processRadius(df,loadDate = loadDate )

    assert(radius.take(1), ReferenceData.radiusStage(loadDate))
  }

  "FlightLeg processing" should "clean FlightLeg input" in {
    val csvReader = CSVReader("src/test/resources/data/G_2020-02-12_03-35-07_flightleg.csv",
      header = false,
      schema = Some(InputStructures.flightLegStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" )

    val df = csvReader.read()

    val stage = new StageProcess()

    val loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now())
    val flightLeg = stage.processFlightLeg(df,loadDate = loadDate )

    assert(flightLeg.take(1), ReferenceData.flightLegStage(loadDate))
  }




  "Order DB csv reader" should "read data properly" in {
    val csvReader = CSVReader("src/test/resources/data/cptm_ta_f_wlan_orderdb.20200212.csv",
      header = false,
      schema = Some(InputStructures.orderdbStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
      dateFormat = "yyyy-MM-dd")

    import spark.implicits._

    val df =  csvReader.read().as[InputTypes.OrderDB].filter("ta_id = 79746017804843337512").first()

    assert(df == ReferenceData.orderDb(14926, Timestamp.valueOf("2020-02-12 04:08:01")))
  }

  "Voucher csv reader" should "read data properly" in {
    val csvReader = CSVReader("src/test/resources/data/cptm_ta_f_wlif_map_voucher.20200212.csv",
      header = false,
      schema = Some(InputStructures.mapVoucherStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
      dateFormat = "yyyy-MM-dd")

    import spark.implicits._

    val df =  csvReader.read().as[InputTypes.MapVoucher].first()

    assert(df == ReferenceData.voucher)
  }

  "Exchange rates csv reader" should "read data properly" in {
    val csvReader = CSVReader("src/test/resources/data/cptm_ta_t_exchange_rates.csv",
      header = false,
      schema = Some(InputStructures.exchangeRatesStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
      dateFormat = "yyyy-MM-dd")

    import spark.implicits._

    val df =  csvReader.read().as[InputTypes.ExchangeRates].first()

    assert(df == ReferenceData.exchangeRates)
  }


}
