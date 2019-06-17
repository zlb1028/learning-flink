package com.realtime.flink.streaming.partition

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}


object PartitioningTransformation {

  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val dataStream: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))

    val shuffleStream = dataStream.shuffle

    val rebalanceStream = dataStream.rebalance

    val rescaleStream = dataStream.rescale

    val customStream = dataStream.partitionCustom(customPartitioner, 1)

    //输出计算结果
    customStream.print()

    env.execute("Streaming PartitioningTransformation")
  }

  object customPartitioner extends Partitioner[String] {
    //获取随机数生成器
    val r = scala.util.Random

    override def partition(key: String, numPartitions: Int): Int = {
      //定义分区策略，key中如果包含a则放在0分区中，其他情况则根据Partitions
      if (key.contains("a")) 0 else r.nextInt(numPartitions)

    }
  }

}
