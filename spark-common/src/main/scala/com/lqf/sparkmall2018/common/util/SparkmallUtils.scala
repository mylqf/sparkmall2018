package com.lqf.sparkmall2018.common.util

object SparkmallUtils {

  def isNotEmpty(content :String):Boolean={

    content!=null && !"".equals(content.trim)
  }

}
