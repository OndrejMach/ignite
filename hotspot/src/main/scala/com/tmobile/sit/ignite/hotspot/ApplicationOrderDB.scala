package com.tmobile.sit.ignite.hotspot

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.LongType

object ApplicationOrderDB  {
  def getWlanHostpotIDsSorted(data: DataFrame) : DataFrame = {
    data
      .select(col("_c0").cast(LongType))
      .sort(desc("_c0"))
      .distinct()
  }

  def main(args: Array[String]): Unit = {

    //val sparkSession = getSparkSession()
  //
    //val dataHotspotIDs = sparkSession
     // .read
      //.option("delimiter", "~")
      //.csv("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/cptm_ta_d_wlan_hotspot.csv")

    //println(getWlanHostpotIDsSorted(dataHotspotIDs).first().getLong(0))

    //val inputMPS = sparkSession
     // .read
     // .text("/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/hotspot/data/input/TMO.MPS.DAY.20200408*.csv")
    //inputMPS.printSchema()

    //val inputFiltered = inputMPS.filter(col("value").startsWith("D;"))
    //inputFiltered.show(false)

  }


}
