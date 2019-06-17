package com.realtime.flink.streaming.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinWindowsProcessFunction {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建数组数据集
    //指定系统时间概念为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.fromCollection(List(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1))).assignAscendingTimestamps(t => t._2)

    val staticStream = inputStream.keyBy(_._1).timeWindow(Time.seconds(10)).process(new StaticProcessFunction)

    inputStream.keyBy(_._1).timeWindow(Time.seconds(10))
    class StaticProcessFunction
      extends ProcessWindowFunction[(String, Long, Int), (String, Long, Long, Long, Long, Long), String, TimeWindow] {
      override def process(key: String,
                           ctx: Context,
                           vals: Iterable[(String, Long, Int)],
                           out: Collector[(String, Long, Long, Long, Long, Long)]): Unit = {
        //定义求和，最大值,最小值，平均值，窗口时间逻辑
        val sum = vals.map(_._2).sum
        val min = vals.map(_._2).min
        val max = vals.map(_._2).max
        var avg = sum / vals.size
        val windowEnd = ctx.window.getEnd
        //通过out.collect返回计算好的结果
        out.collect((key, min, max, sum, avg, windowEnd))
      }
    }
    staticStream.print()

  }


}
