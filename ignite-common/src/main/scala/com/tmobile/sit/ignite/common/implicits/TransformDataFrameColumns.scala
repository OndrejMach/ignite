package com.tmobile.sit.ignite.common.implicits

import org.apache.spark.sql.DataFrame

object TransformDataFrameColumns {
  implicit class TransformColumnNames(df : DataFrame) {
    def columnsToUpperCase() : DataFrame = {
      df.toDF(df.columns.map(_.toUpperCase()):_*)
    }
  }
}
