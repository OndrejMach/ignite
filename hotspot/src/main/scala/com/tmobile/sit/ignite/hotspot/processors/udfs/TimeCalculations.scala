package com.tmobile.sit.ignite.hotspot.processors.udfs

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.temporal.ChronoUnit

import com.tmobile.sit.ignite.hotspot.processors.fileprocessors.SessionMetrics

import sys.process._
import scala.annotation.strictfp
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DirtyStuff{
  val encode = (encoderPath: String,data: String) => {
    s"${encoderPath} ${data}" !!
  }
  val precisionCorrection = (number: Double) => {
    val arr = number.toString.split("\\.")
    val prec = if (arr(1).length == 1) {
      arr(1)+"0"
    } else {
      arr(1)
    }
    s"${arr(0)}.${prec}"
  }
  val padPercentage = (s: String) => {
    s.replaceAll("\\.0%", "\\.00%")
  }

  val removeTrailing0s = (l: Double) => {
    val arr = l.toString.split("\\.")
    if (arr(1) == "0") {
      arr(0)
    } else {
      l.toString
    }
  }
}



@strictfp
object TimeCalculations {
  @strictfp
  val toQuartersUnixTime = (startTS: Long, eventTS: Long, SesVolume: Long, SesDuration: Long, processingDate: Timestamp ) => {
    val odate_start = processingDate.toLocalDateTime.toEpochSecond(ZoneOffset.UTC) + 2*3600
    val odate_end = odate_start + 86400

    def isDateValid(end_quarter: Long): Boolean = {
      ((end_quarter - 900) >= odate_start) && (end_quarter <= odate_end)
    }
    var end_quarter = startTS + 900 - (startTS % 900)  //*in->session_start_ts + 900 - (*in->session_start_ts % 900);
    val volume_per_sec =  if (SesDuration!=0) SesVolume.toDouble/SesDuration else 0 //(double) *in->session_volume / *in->session_duration;
    var quarter_of_an_hour_id  = ((startTS - (startTS % 900)) % 86400)/60 //*out->quarter_of_an_hour_id 	= ((*in->session_start_ts - (*in->session_start_ts % 900)) % 86400) / 60;

    var ret = ListBuffer[SessionMetrics]()

    if (eventTS <= end_quarter) {
      if (isDateValid(end_quarter)) ret += SessionMetrics(start_flag = 1, end_flag = 1, duration = SesDuration/60, volume = SesVolume.toDouble,quarterID = quarter_of_an_hour_id)
    } else {
      if (isDateValid(end_quarter)) ret += SessionMetrics(start_flag = 1, end_flag = 0, duration = (end_quarter-startTS)/60, volume =  (volume_per_sec*(end_quarter-startTS)),quarterID = quarter_of_an_hour_id)
      var stop = false
      while((end_quarter < eventTS) && !stop) {
        end_quarter += 900
        quarter_of_an_hour_id = (quarter_of_an_hour_id + 15) % 1440
        if (end_quarter >= eventTS) {
          if (isDateValid(end_quarter)) ret+= SessionMetrics(start_flag = 0, end_flag = 1, volume = volume_per_sec * (eventTS + 900 - end_quarter), duration = (eventTS + 900 - end_quarter) / 60, quarterID = quarter_of_an_hour_id )
          stop = true
        } else {
          if (isDateValid(end_quarter)) ret+=SessionMetrics(start_flag = 0, end_flag = 0, volume = volume_per_sec*900, duration = 15,quarterID = quarter_of_an_hour_id )
        }
      }
    }
    ret


    /*
    else {
  *out->session_duration 	= (end_quarter - *in->session_start_ts) / 60; // in minutes
  *out->session_volume 		= volume_per_sec * (end_quarter - *in->session_start_ts) ;
  *out->start_flag 		= 1;
  *out->end_flag 		= 0;

  // output only start record of current ODATE
  if (end_quarter - 900 >= odate_start && end_quarter <= odate_end) add_record();

  // normalize
  while (end_quarter < *in->session_event_ts) {
    *out->quarter_of_an_hour_id = (*out->quarter_of_an_hour_id + 15) % 1440; //modulo because of midnight
    end_quarter += 900;
    // case event_ts is within this quarter
    if (end_quarter >= *in->session_event_ts) {
      *out->session_duration 	= (*in->session_event_ts + 900 - end_quarter) / 60; // in minutes
      *out->session_volume 	= volume_per_sec * (*in->session_event_ts + 900 - end_quarter);
      *out->start_flag 		= 0;
      *out->end_flag 		= 1;
      // output only end record of current ODATE
      if (end_quarter - 900 >= odate_start && end_quarter <= odate_end) add_record();
      break;
    }
    // case event_ts continues after this quarter
    else {
      *out->session_duration 	= 15; // in minutes
      *out->session_volume 	= volume_per_sec * 900;
      *out->start_flag 		= 0;
      *out->end_flag 		= 0;
      // output only middle record of current ODATE
      if (end_quarter - 900 >= odate_start && end_quarter <= odate_end) add_record();
    }
  }
*/
  }
}
