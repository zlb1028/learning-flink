package com.realtime.flink.streaming.multi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object SplitOperator {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    //创建数据集
    val dataStream1: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))
    //合并两个DataStream数据集
    val splitedStream: SplitStream[(String, Int)] = dataStream1.split(t => if (t._2 % 2 == 0) Seq("even") else Seq("odd"))
    //删选出偶数数据集
    val evenStream: DataStream[(String, Int)] = splitedStream.select("even")
    //删选出奇数数据集
    val oddStream: DataStream[(String, Int)] = splitedStream.select("odd")
    //删选出奇数和偶数数据集
    val allStream: DataStream[(String, Int)] = splitedStream.select("even", "odd")

    evenStream.print()
    oddStream.print()
    allStream.print()

    env.execute("Streaming UnionOperator")
  }

}
