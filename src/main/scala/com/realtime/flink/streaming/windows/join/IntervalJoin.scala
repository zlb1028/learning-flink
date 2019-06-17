package com.realtime.flink.streaming.windows.join

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoin {
  def main(args: Array[String]): Unit = {

    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建黑色元素数据集
    val blackStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))
    //创建白色元素数据集
    val whiteStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (1, 1L), (3, 4L))
    //通过Join方法将两个数据集进行关联
    val windowStream: DataStream[String] = blackStream.keyBy(_._1)
      .intervalJoin(whiteStream.keyBy(_._1)).between(Time.milliseconds(-2), Time.milliseconds(1))
      .process(new ProcessWindowFunciton())

    class ProcessWindowFunciton extends ProcessJoinFunction[(Int, Long), (Int, Long), String] {
      override def processElement(in1: (Int, Long), in2: (Int, Long), context: ProcessJoinFunction[(Int, Long), (Int, Long), String]#Context, collector: Collector[String]): Unit = {
        collector.collect(in1  + ":" + (in1._2 + in2._2))
      }
    }
  }

}
