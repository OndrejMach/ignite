package com.tmobile.sit.ignite.rcse

import org.apache.spark.sql.functions.{lit, map, explode, col}

object Application extends App {
  val sparkSession = getSparkSession()

  case class Schema1(terminal_id: Int, manufacturer: String, model: String)

  import sparkSession.implicits._

  val df = Seq(
    Schema1(1, "nokia", "3310"),
    Schema1(2, "ericsson", "8"),
    Schema1(3, "huawei", "p9")
  ).toDF()

  df.show()

  val exploded = df
    .withColumn("specs", map(
      lit("MANUFACTURER"), col("manufacturer"),
      lit("MODEL"), col("model"))
    )
    .select(col("terminal_id"), explode(col("specs")))

  exploded
    .show()


}
