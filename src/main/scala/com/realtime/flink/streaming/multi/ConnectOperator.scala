package com.realtime.flink.streaming.multi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, ConnectedStreams, DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


object ConnectOperator {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    //    env.setParallelism(1)

    //创建不同的数据集
    val dataStream1: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))

    val dataStream2: DataStream[Int] = env.fromElements(1, 2, 4, 5, 6)

    //连接两个DataStream数据集
    val connectedStream: ConnectedStreams[(String, Int), Int] = dataStream1.connect(dataStream2)

    val resultStream = connectedStream.map(new CoMapFunction[(String, Int), Int, (Int, String)] {
      override def map1(in1: (String, Int)): (Int, String) = {
        (in1._2, in1._1)
      }

      override def map2(in2: Int): (Int, String) = {
        (in2, "default")
      }
    }
    )

    //    resultStream.print()

    val resultStream2 = connectedStream.flatMap(new CoFlatMapFunction[(String, Int), Int, (String, Int, Int)] {
      //定义共享变量
      var number = 0

      //定义第一个数据集处理函数
      override def flatMap1(in1: (String, Int), collector: Collector[(String, Int, Int)]): Unit = {
        collector.collect((in1._1, in1._2, number))
      }

      //定义第二个数据集处理函数
      override def flatMap2(in2: Int, collector: Collector[(String, Int, Int)]): Unit = {
        number = in2
      }
    }

    )

    resultStream2.print()

    // 通过keyby函数根据指定的key连接两个数据集
    val keyedConnect: ConnectedStreams[(String, Int), Int] = dataStream1.connect(dataStream2).keyBy(1, 0)
    // 通过broadcast关联两个数据集
    val broadcastConnect: BroadcastConnectedStream[(String, Int), Int] = dataStream1.connect(dataStream2.broadcast())

    env.execute("Streaming ConnectorOperator")
  }

}
