package com.lqf.sparkmall2018.common.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  def getStringByTimestamp(ts:Long,f:String)={
    getStringByDate(new Date(ts),f)
  }

  def getStringByDate(d:Date,f:String)={

    val sdf = new SimpleDateFormat(f)
    sdf.format(d)
  }

}
