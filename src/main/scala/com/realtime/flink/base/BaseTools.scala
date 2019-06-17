package com.realtime.flink.base

import com.realtime.flink.WebLogData
import org.junit.Test


trait BaseTools {

  def generateWebData(): List[WebLogData] = {

    val webLogDataList: List[WebLogData] = List()
    for (i <- 1 to 10) {
      webLogDataList.+:(new WebLogData(i, i.toString, "address"))
    }
    webLogDataList
  }

  @Test
  def testGenerateWebData() {
    val list: List[WebLogData] = generateWebData
    list.foreach(println)
  }
}
