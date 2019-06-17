package com.realtime.flink.streaming.multi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}


object UnionOperator {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    //创建不同的数据集
    val dataStream_01: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))

    val dataStream_02: DataStream[(String, Int)] = env.fromElements(("d", 1), ("s", 2), ("a", 4), ("e", 5), ("a", 6))

    val dataStream_03: DataStream[(String, Int)] = env.fromElements(("a", 2), ("d", 1), ("s", 2), ("c", 3), ("b", 1))

    //合并两个DataStream数据集
    val unionStream = dataStream_01.union(dataStream_02)
    //合并多个DataStream数据集
    val allUnionStream = dataStream_01.union(dataStream_02, dataStream_03)

    env.execute("Streaming UnionOperator")
  }

}
