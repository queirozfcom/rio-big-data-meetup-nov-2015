package com.queirozf.utils

import org.apache.spark.sql.functions._
import org.joda.time.{LocalTime, DateTime}

/**
 * Created by felipe on 09/11/15.
 */
object UDF {

  /** ********************************
    * UDFs: User-Defined Functions
    * **********************************/

  // returns the length of given text
  val stringLength = udf { txt: String =>
    txt.length.toDouble
  }

  // outputs 1.0 if given timestamp represents a weekday, 0.0 otherwise
  val timestampIsWeekDay = udf { unixTimestamp: Long =>

    val date = new DateTime(unixTimestamp * 1000L)

    date.getDayOfWeek match {
      case 1 => 1.0
      case 2 => 1.0
      case 3 => 1.0
      case 4 => 1.0
      case 5 => 1.0
      case 6 => 0.0
      case 7 => 0.0
    }
  }

  // outputs 1.0 if given timestamp represents weekend, 0.0 otherwise
  val timestampIsWeekend = udf { unixTimestamp: Long =>

    val date = new DateTime(unixTimestamp * 1000L)

    date.getDayOfWeek match {
      case 1 => 0.0
      case 2 => 0.0
      case 3 => 0.0
      case 4 => 0.0
      case 5 => 0.0
      case 6 => 1.0
      case 7 => 1.0
    }
  }

  // outputs 1.0 if given timestamp represents a time before noon (AM), 0.0 otherwise
  val timestampIsAM = udf { unixTimestamp: Long =>

    val elevenFiftyNine = new LocalTime(11, 59, 59)
    val date = new DateTime(unixTimestamp * 1000L)

    date.toLocalTime.compareTo(elevenFiftyNine) match {
      case -1 => 1.0
      case 1 => 0.0
      case _ => 0.0 // just to make sure nothing gets through
    }
  }

  // outputs 1.0 if given timestamp represents a time after noon (PM), 0.0 otherwise
  val timestampIsPM = udf { unixTimestamp: Long =>

    val elevenFiftyNine = new LocalTime(11, 59, 59)
    val date = new DateTime(unixTimestamp * 1000L)

    date.toLocalTime.compareTo(elevenFiftyNine) match {
      case -1 => 0.0
      case 1 => 1.0
      case _ => 1.0 // just to make sure nothing gets through
    }
  }

  // given a pair of ints, return the first divided by the second
  val getRatio = udf { pair: Seq[Long] =>

    if (pair(1) == 0) 0.0
    else pair(0).toDouble / pair(1).toDouble
  }

  val len = udf { str: String => str.length }

  val toLower = udf { str: String => str.toLowerCase }

}
