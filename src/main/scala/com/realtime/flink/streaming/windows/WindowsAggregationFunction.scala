package com.realtime.flink.streaming.windows

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsAggregationFunction {
  def main(args: Array[String]): Unit = {

    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.fromElements(("hello", 21L), ("flink", 1L), ("hadoop", 4L))

    val aggregateWindowStream = inputStream
      .keyBy(_._1)
      //指定窗口类型
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
      //指定聚合函数逻辑，将根据ID将第二个字段求平均值
      .aggregate(new MyAverageAggregate)
  }

}

class MyAverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  //定义createAccumulator为两个参数的元祖
  override def createAccumulator() = (0L, 0L)
  //定义输入数据累加到accumulator的逻辑
  override def add(value: (String, Long), accumulator: (Long, Long)) =
    (accumulator._1 + value._2, accumulator._2 + 1L)
  //根据累加器得出结果
  override def getResult(accumulator: (Long, Long)) = accumulator._1 / accumulator._2
  //定义累加器合并的逻辑
  override def merge(a: (Long, Long), b: (Long, Long)) =
    (a._1 + b._1, a._2 + b._2)
}
