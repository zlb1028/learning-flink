package com.realtime.flink.streaming.basic

import com.realtime.flink.base.BaseTools
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}


object FoldOperator extends BaseTools {

  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)


    val dataStream: DataStream[(Int, Int)] = env.fromElements(
      (1, 5), (2, 1), (2, 4), (1, 3))


  }
}
