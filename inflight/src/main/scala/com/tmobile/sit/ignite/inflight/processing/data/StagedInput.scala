package com.tmobile.sit.ignite.inflight.processing.data

import org.apache.spark.sql.Dataset
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes


case class StagedInput(
                      radius: Dataset[StageTypes.Radius],
                      airport: Dataset[StageTypes.Airport],
                      aircraft: Dataset[StageTypes.Aircraft],
                      flightLeg: Dataset[StageTypes.FlightLeg],
                      oooi: Dataset[StageTypes.Oooi],
                      airline: Dataset[StageTypes.Airline],
                      realm: Dataset[StageTypes.Realm]
                      )
