package com.tmobile.sit.ignite.common

final case class InputRowCSV(id: Option[Int], number: Option[Int], string: Option[Int])
final case class InputRowMDB(BUCHMONAT: String, GESPR_DAUER: String,ANZ_VERB: String, EURO: String, LEISTUNGSMONAT: String )
final case class InputRowExcel(BUCHMONAT: Option[String], GESPR_DAUER: Option[Double],ANZ_VERB: Option[Double], EURO: Option[Double], LEISTUNGSMONAT: Option[String])


object ReferenceData {

  def  csv_with_header = List( InputRowCSV(Some(1),Some(2),Some(3)))
  def mdb_data_no_schema = List(
    InputRowMDB("201905",	"2993",	"37",	"115.26","201903"),
    InputRowMDB("201905",	"11264",	"66",	"185.51","201902"),
    InputRowMDB("201905",	"327071",	"2935",	"8392.09","201905"),
    InputRowMDB("201905",	"66302",	"616",	"1751.43","201904"),
    InputRowMDB("201905",	"529",	"6",	"18.39","201901")
  )
  def mdb_data_with_schema = List(
    InputRowExcel(Some("201905"),	Some(2993),	Some(37),	Some(115.26),Some("201903")),
    InputRowExcel(Some("201905"),	Some(11264),	Some(66),	Some(185.51),Some("201902")),
    InputRowExcel(Some("201905"),	Some(327071),	Some(2935),	Some(8392.09),Some("201905")),
    InputRowExcel(Some("201905"),	Some(66302),	Some(616),	Some(1751.43),Some("201904")),
    InputRowExcel(Some("201905"),	Some(529),	Some(6),	Some(18.39),Some("201901"))
  )

  def excel_data = List(
    InputRowExcel(Some("201912"),Some(701.0),Some(7.0),Some(19.1), Some("201911")   ),
    InputRowExcel(Some("201912"),Some(85.0),Some(1.0),Some(3.34), Some("201912")   )
  )

}
