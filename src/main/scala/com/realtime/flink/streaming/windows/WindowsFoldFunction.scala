package com.realtime.flink.streaming.windows

import org.apache.flink.api.common.functions.{AggregateFunction, FoldFunction, ReduceFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsFoldFunction {
  def main(args: Array[String]): Unit = {

    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.fromElements((1, 21L), (3, 1L), (5, 4L))

    val foldWindowStream = inputStream
      .keyBy(_._1)
      //指定窗口类型
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
      //指定聚合函数逻辑,
      .fold("flink") { (acc, v) => acc + v._2 }
    foldWindowStream.print()

    env.execute()
  }

}


