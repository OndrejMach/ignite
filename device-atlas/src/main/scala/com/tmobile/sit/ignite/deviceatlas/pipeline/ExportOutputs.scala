package com.tmobile.sit.ignite.deviceatlas.pipeline

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr

trait ExportGeneration extends Logger {
  def generateSpec(d_terminal_spec : DataFrame, ODATE: String, outputPath : String) : DataFrame
  def generateTac(d_tac : DataFrame, ODATE: String, outputPath : String) : DataFrame
}

class ExportOutputs (implicit sparkSession: SparkSession) extends ExportGeneration {

  import sparkSession.implicits._
  final val max_date = "4712-12-31"
  val today = Calendar.getInstance().getTime()
  val formatInt = new SimpleDateFormat("yyyyMMddHHmmss")

  override def generateSpec(d_terminal_spec: DataFrame, ODATE: String, outputPath: String): DataFrame = {
/* EVL logic pseudo code:
    input df d_terminal_spec = cptm_ta_d_terminal_spec.csv

    records with valid_to = MAXDATE &
            !(terminal_spec_name == "LAUNCH_DATE" && terminal_spec_value == "N/A") &
            terminal_id != 9002
    --> "L" ; terminal_id ; str_replace(terminal_spec_name, ';', ',') ; str_replace((terminal_spec_value).substr(0,50), ';', ',')


    write cptm_ta_d_terminal_spec_$ODATE.csv
    add last line:
    echo "X;Endgeraeteeigenschaften;$TIMESTAMP;$NO_OF_RECS;Generated from Terminal Database v.5;$TIMESTAMP" >>$FILE_TERM_EXP
    write FILE_TERM_RDY:
    echo "X;Endgeraeteeigenschaften;$TIMESTAMP;$NO_OF_RECS;Generated from Terminal Database v.5;$TIMESTAMP" >$FILE_TERM_RDY
*/
    logger.info("Generating data for cptm_ta_d_terminal_spec")
    val filtered_term_spec = d_terminal_spec
      .where(s"valid_to >= '${max_date}' AND NOT(terminal_spec_name == 'LAUNCH_DATE' AND terminal_spec_value == 'N/A') AND terminal_id != 9002")

    val final_set = filtered_term_spec
      .selectExpr("'L' as flag",
        "terminal_id",
        "REPLACE(terminal_spec_name, ';', ',') as terminal_spec_name",
        "REPLACE(LEFT(terminal_spec_value, 50), ';', ',') as terminal_spec_value")
      .na.fill("N/A", Seq("terminal_spec_value"))
      .na.replace(Seq("terminal_spec_value"),Map(""->"N/A"))

    val rec_count = final_set.count()
    val last_line =Seq(("X;Endgeraeteeigenschaften",s"${formatInt.format(today)};$rec_count","Generated from Terminal Database v.6", s"${formatInt.format(today)}"))
      .toDF()

    final_set
      .union(last_line)

  }

  override def generateTac(d_tac: DataFrame, ODATE: String, outputPath: String): DataFrame = {
/*    EVL logic pseudo code:
  from d_tac: records with valid_to = MAXDATE & status ACTIVE or NO INFO
        --> a) few columns to TAC_UNCHANGED   [terminal_id, tac_code, id, manufacturer, model]
        --> b) same + TAC6 to TAC_CODE_6

        TAC_UNCHANGED:
         ignore tac_code = "35730808" && terminal_id = 9002
         tac_code, id=[terminal_id] to TAC_U_FILT

        TAC_CODE_6:
         dedup on "terminal_id,tac_code,id,manufacturer,model"
         sort by tac_code [taccode6]
         group by tac_code and count for each group
         if cnt==1 and id not null and not "35730808" && 9002
             tac_code and id to TAC_6_CNT else drop

        Cat TAC_U_FILT TAC_6_CNT -->
        --> uniq sort on tac_code, id=[terminal_id]
   ----------------- the rest by DF writer?
        --> write cptm_vi_d_tac_terminal_$ODATE.csv

        add last line:
        echo "X;Terminalmapping;$TIMESTAMP;$NO_OF_RECS;Generated from Terminal Database v.5;$TIMESTAMP" >>$FILE_TAC_EXP
        write FILE_TAC_RDY:
        echo "X;Terminalmapping;$TIMESTAMP;$NO_OF_RECS;Generated from Terminal Database v.5;$TIMESTAMP" >$FILE_TAC_RDY

*/

    logger.info("Generating data for cptm_vi_d_tac_terminal")
    val filtered_tac = d_tac
      .where(s"valid_to >= '${max_date}' AND (status == 'ACTIVE' OR (LEFT(status, 2) == 'NO' AND RIGHT(status, 4) == 'INFO'))")
      .select("terminal_id", "tac_code", "id", "manufacturer", "model")
      .withColumn("tac_code6", expr("substr(tac_code, 0, 6)"))
      .where("tac_code != '35730808' AND terminal_id != 9002")

    val tac6_cnt = filtered_tac.dropDuplicates("terminal_id" ,"tac_code6","id","manufacturer","model")
      .groupBy("tac_code6")
      .count()
      .where("count == 1")
      .join(filtered_tac, Seq("tac_code6"), "inner")
      .select("tac_code6", "terminal_id")

    val final_set = filtered_tac.select("tac_code", "terminal_id")
      .union(tac6_cnt)
      .sort("tac_code", "terminal_id")
      .dropDuplicates("tac_code", "terminal_id")

    val rec_count = final_set.count()

    val last_line = Seq((s"X;Terminalmapping;${formatInt.format(today)};$rec_count;Generated from Terminal Database v.6", s"${formatInt.format(today)}"))
      .toDF()

    final_set
      .union(last_line)

  }

}
