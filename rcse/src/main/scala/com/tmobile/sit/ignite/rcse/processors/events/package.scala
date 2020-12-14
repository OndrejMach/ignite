package com.tmobile.sit.ignite.rcse.processors

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, when}

package object events {
  def c(c: Column) :Column = when(c.isNull, lit("")).otherwise(c)
}
