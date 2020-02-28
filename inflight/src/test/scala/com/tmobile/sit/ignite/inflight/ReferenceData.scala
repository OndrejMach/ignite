package com.tmobile.sit.ignite.inflight

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.datastructures.InputStructures
import com.tmobile.sit.ignite.inflight.datastructures.StageStructures
import org.apache.spark.sql.SparkSession

object ReferenceData {
  val inputAircraft = List(InputStructures.Aircraft(Some("B-16732"),Some("Boeing"), Some("B777-300ER"), None, Some(0), None,Some("EVA"), Some("B773"), Some("773"),Some("yes"), Some(4996), Some("DE_TW5459")))
  val stagedAircraft = List(StageStructures.Aircraft("B-16732", "N/A", "Boeing","B777-300ER", None, 0, "EVA", "B773","773", "yes", 4996, "DE_TW5459"))

  val stagedAirline = List(StageStructures.Airline(Some("SCO"), "Scoot", Some("TZ"), None))

  val stagedAirport = List(StageStructures.Airport(Some("AYGA"),"GOROKA", Some("GKA"), Some("GOROKA"), Some("PAPUA NEW GUINE"), Some(-6.08167), Some(145.392), Some("no") ))

  val stagedRealm = List(StageStructures.Realm(Some("t-online.de"), Some("t-online.de"), Some("postpaid")))

  def stagedOooi (loadDate: Timestamp) = List(StageStructures.Oooi(Some(9842350), Some("arrive_gate"), Some(6800409), Some("0"), Some(55005), Some("ETD"), Some("A6-BLJ"), Some("418"),
    Some("WMKK"), Some("NNNN"), Some(Timestamp.valueOf("2019-10-27 02:26:00")), Some(Timestamp.valueOf("2019-10-27 02:30:27")), Some(0), Some(loadDate)))
}
