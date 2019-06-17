package com.realtime.flink.streaming.basic

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment, _}


object MapOperator {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val dataStream = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))
    //指定Reduce计算表达式
    val mapStream_01: DataStream[(String, Int)] = dataStream.map(t => (t._1, t._2 + 1))
    //指定MapFunction
    val mapStream_02: DataStream[(String, Int)] = dataStream.map(new MapFunction[(String, Int), (String, Int)] {
      override def map(t: (String, Int)): (String, Int) = {
        (t._1, t._2 + 1)
      }
    })

    //输出计算结果
    mapStream_01.print()

    //输出计算结果
    mapStream_02.print()

    env.execute("Streaming ReduceOperator")
  }

}
