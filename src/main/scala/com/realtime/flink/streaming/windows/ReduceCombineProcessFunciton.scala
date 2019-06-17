package com.realtime.flink.streaming.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by zhanglibing on 2019/2/17
  */
object ReduceCombineProcessFunciton {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建数组数据集
    //指定系统时间概念为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.fromCollection(List(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1))).assignAscendingTimestamps(t => t._2)


    inputStream.keyBy(_._1).timeWindow(Time.seconds(10))

    val result = inputStream
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce(
        //定义ReduceFunction，完成求取最小值的逻辑
        (r1: (String, Long, Int), r2: (String, Long, Int)) => {
          if (r1._2 > r2._2) r2 else r1
        }
        , //定义ProcessWindowsFunciton，完成对窗口元数据的采集
        (key: String,
         window: TimeWindow,
         minReadings: Iterable[(String, Long, Int)],
         out: Collector[(Long, (String, Long, Int))]) => {
          val min = minReadings.iterator.next()
          //采集窗口结束时间和最小值对应的数据元素
          out.collect((window.getEnd, min))
        })
  }
}