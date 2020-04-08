package com.tmobile.sit.ignite.inflight.processing.data

import org.apache.spark.sql.Dataset
import com.tmobile.sit.ignite.inflight.datastructures.StageTypes

/**
 * Just a wraper class - It's and output for full data processing.
 * @param radius - radius data
 * @param airport - airport data
 * @param aircraft - aircraft data
 * @param flightLeg - flightLeg data
 * @param oooi - oooi data
 * @param airline - airline data
 * @param realm - realm data
 */
case class StagedDataForFullOutput(
                        radius: Dataset[StageTypes.Radius],
                        airport: Dataset[StageTypes.Airport],
                        aircraft: Dataset[StageTypes.Aircraft],
                        flightLeg: Dataset[StageTypes.FlightLeg],
                        oooi: Dataset[StageTypes.Oooi],
                        airline: Dataset[StageTypes.Airline],
                        realm: Dataset[StageTypes.Realm]
                      )
