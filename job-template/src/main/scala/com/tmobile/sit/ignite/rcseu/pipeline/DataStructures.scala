package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

case class InputData(people: Reader, salaryInfo: Reader)

case class PreprocessedData(peopleData: DataFrame, salaryData: DataFrame)
