package com.realtime.flink.streaming.state

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
  * Created by zhanglibing on 2019/2/20
  */
object MapStateFunciton {

  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建元素数据集
    val inputStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))

    val counts: DataStream[(Int, Int)] = inputStream
      .keyBy(_._1)
      //指定输入参数类型和状态参数类型
      .mapWithState((in: (Int, Long), count: Option[Long]) =>
        count match {
            //输出key，count，并在原来的count数据上累加
          case Some(c) => ((in._1, c), Some(c + in._2))
            //如果输入状态为空，则将指标填入
          case None => ((in._1, 0), Some(in._2))
        })

    counts.print()
  }

}
