package com.realtime.flink.streaming.windows

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object TumbliingWindow {

  def main(args: Array[String]): Unit = {

    case class WebLog(id: Long, content: String, trackTime: Long)

    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream = env.fromElements(WebLog(2, "flink", 21L), WebLog(4, "flink", 1L), WebLog(3, "flink", 4L))

    inputStream.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .process(...)

  }

}
