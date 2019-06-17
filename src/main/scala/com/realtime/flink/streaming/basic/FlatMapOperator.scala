package com.realtime.flink.streaming.basic

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


object FlatMapOperator {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val dataStream = env.fromElements("a,b,c", "d,f,e", "c,d,a", "c,b,d")

    //指定flatmap计算表达式
    val flatMapStream_01: DataStream[String] = dataStream.flatMap(_.split(","))

    //指定FlatMapFunction
    val flatMapStream_02: DataStream[String] = dataStream.flatMap(new FlatMapFunction[String, String] {
      override def flatMap(t: String, collector: Collector[String]): Unit = {
        t.split(",").foreach(collector.collect)
      }
    })

    //输出计算结果
    flatMapStream_01.print()


    //输出计算结果
    flatMapStream_02.print()

    env.execute("Streaming ReduceOperator")
  }

}
