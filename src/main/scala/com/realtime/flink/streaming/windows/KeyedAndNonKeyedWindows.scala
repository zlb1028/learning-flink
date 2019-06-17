package com.realtime.flink.streaming.windows

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object KeyedAndNonKeyedWindows {
  def main(args: Array[String]): Unit = {
    case class SensorEvent(id: Long, name: String, timestamp: Long)

    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorEvent = env.fromElements(SensorEvent(11, "Normal", 1L), SensorEvent(11, "Normal", 1L), SensorEvent(11, "Normal", 1L))

//    sensorEvent.keyBy(event => event.id).window(new MyWindowsAssigner())
//    sensorEvent.windowAll(new MyAllWindowsAssigner())
  }

}
