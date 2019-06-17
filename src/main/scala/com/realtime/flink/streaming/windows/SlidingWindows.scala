package com.realtime.flink.streaming.windows

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by zhanglibing on 2019/2/17
  */
object SlidingWindows {
  def main(args: Array[String]): Unit = {
    case class WebLog(id: Long, content: String, trackTime: Long)

    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream = env.fromElements(WebLog(2, "flink", 21L), WebLog(4, "flink", 1L), WebLog(3, "flink", 4L))

//    inputStream.keyBy(_.id)
//      //
//      .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(10)))
//      .process(...)
//
//    inputStream.keyBy(_.id)
//      //
//      .window(SlidingProcessingTimeWindows.of(Time.hours(1),Time.minutes(10)))
//      .process(...)
//
//    inputStream.keyBy(_.id)
//      //
//      .timeWindow(Time.hours(1),Time.minutes(10)))
//      .process(...)
  }


}
