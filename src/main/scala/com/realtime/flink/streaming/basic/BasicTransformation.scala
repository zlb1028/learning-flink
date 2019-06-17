package com.realtime.flink.streaming.basic

import com.realtime.flink.WordCountData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object BasicTransformation {

  def main(args: Array[String]): Unit = {

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val inputData = List(1,2,3,4)

    val dataStream:DataStream[Int] = env.fromCollection(inputData)

    dataStream.print()

  }
}
