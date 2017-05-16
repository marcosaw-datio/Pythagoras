package com.datio.ingesta.util

import java.text.DecimalFormat

/**
  * Created by marcos on 11/05/17.
  */
object NumberUtils {

  /**
    *
    * @param sn a number in string type
    * @return a number in double value
    */
  def numberFormatter(sn: String): Double = {
    //val customFormatter = new DecimalFormat("###,###.##")

    // generic DecimalFormat without pattern
    val customFormatter = new DecimalFormat()

    // TODO: validate decimal before conversion
    //if (customFormatter.isParseBigDecimal())

    return customFormatter.parse(sn).doubleValue()
  }

}
