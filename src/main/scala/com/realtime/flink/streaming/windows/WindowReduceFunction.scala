package com.realtime.flink.streaming.windows

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowReduceFunction {
  def main(args: Array[String]): Unit = {

    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.fromElements((2, 21L), (4, 1L), (3, 4L))

    val reduceWindowStream = inputStream
      .keyBy(_._1)
      //指定窗口类型
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
      //指定聚合函数逻辑，将根据ID将第二个字段求和
      .reduce(new ReduceFunction[(Int, Long)] {
      override def reduce(t1: (Int, Long), t2: (Int, Long)): (Int, Long) = {
        (t1._1, t1._2 + t2._2)
      }
    })

  }

  class MinTempFunction extends ReduceFunction[(String, Double)] {
    override def reduce(r1: (String, Double), r2: (String, Double)) = {
      (r1._1, r1._2.min(r2._2))
    }
  }

}
