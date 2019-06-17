package com.realtime.flink.streaming.basic

import com.realtime.flink.WebLogData
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


object AggregationOperator {

  def main(args: Array[String]): Unit = {

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val dataStream = env.fromElements((1, 5), (2, 4), (2, 2), (1, 3), (1, 3))

    val keyedStream: KeyedStream[(Int, Int), Tuple] = dataStream.keyBy(0)

    //滚动计算sum指标
    val sumStream: DataStream[(Int, Int)] = keyedStream.sum(1)

    //滚动计算min指标
    val minStream: DataStream[(Int, Int)] = keyedStream.min(1)

    //滚动计算max指标
    val maxStream: DataStream[(Int, Int)] = keyedStream.max(1)

    //滚动计算minBy指标
    val minByStream: DataStream[(Int, Int)] = keyedStream.minBy(1)

    //滚动计算maxBy指标
    val maxByStream: DataStream[(Int, Int)] = keyedStream.maxBy(1)

    minStream.print()

    minByStream.print()

    maxByStream.print()

    // execute program
    env.execute("Streaming AggregationOperator")

  }

}
