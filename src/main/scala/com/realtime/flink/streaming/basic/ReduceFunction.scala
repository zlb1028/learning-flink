package com.realtime.flink.streaming.basic

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}

object ReduceFunction {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val dataStream = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))

    //指定第一个字段为分区Key
    val keyedStream: KeyedStream[(String, Int), Tuple] = dataStream.keyBy(0)

    //指定Reduce计算表达式
    val reduceStream = keyedStream.reduce { (t1, t2) =>
      (t1._1, t1._2 + t2._2)
    }

    //输出计算结果
//    reduceStream.print()

    //通过实现ReduceFunction匿名类
    val reduceStream1 = keyedStream.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(t1: (String, Int), t2: (String, Int)): (String, Int) = {
        (t1._1, t1._2 + t2._2)
      }
    })

    //输出计算结果
    reduceStream1.print()

    env.execute("Streaming ReduceOperator")
  }

}
