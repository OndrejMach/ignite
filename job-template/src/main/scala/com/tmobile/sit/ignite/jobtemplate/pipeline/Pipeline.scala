package com.tmobile.sit.ignite.jobtemplate.pipeline

import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.jobtemplate.config.Settings
import org.apache.spark.sql.SparkSession

class Pipeline(inputData: InputData, stage: TemplateStageProcessing, core: ProcessingCore, settings: Settings)(implicit sparkSession: SparkSession) {
  def run(): Unit = {

    val inputPeople = inputData.people.read()
    val inputSalary = inputData.salaryInfo.read()

    val preprocessedPeople = stage.preprocessPeople(inputPeople)
    val preprocessedSalary = stage.preprocessSalaryInfo(inputSalary)

    val preprocessedData = PreprocessedData(preprocessedPeople, preprocessedSalary)

    val result = core.process(preprocessedData)


    CSVWriter(data = result,settings.outputPath.get, writeHeader = true)
  }

}
