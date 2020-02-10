package com.tmobile.sit.ignite.jobtemplate.pipeline

import com.tmobile.sit.ignite.common.readers.Reader
import com.tmobile.sit.ignite.common.writers.Writer
import com.tmobile.sit.ignite.jobtemplate.Inputs
import org.apache.spark.sql.DataFrame

class Pipeline(inputData: InputData, stage: TemplateStageProcessing, core: ProcessingCore, writer: Writer) {
  def run(): Unit = {

    val preprocessedData = PreprocessedData(stage.preprocessPeople(inputData.people.read()),stage.preprocessSalaryInfo(inputData.salaryInfo.read()) )

    val result = core.process(preprocessedData)

    writer.writeData(result)
  }

}
