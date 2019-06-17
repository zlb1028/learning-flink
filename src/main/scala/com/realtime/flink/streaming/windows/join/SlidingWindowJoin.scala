package com.realtime.flink.streaming.windows.join

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by zhanglibing on 2019/2/19
  */
object SlidingWindowJoin {

  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建黑色元素数据集
    val blackStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))
    //创建白色元素数据集
    val whiteStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (1, 1L), (3, 4L))
    //通过Join方法将两个数据集进行关联
    val windowStream: DataStream[Long] = blackStream.join(whiteStream)
      .where(_._1) //指定第一个Stream的关联Key
      .equalTo(_._1) //指定第二个Stream的关联Key
      .window(SlidingEventTimeWindows.of(Time.milliseconds(10),Time.milliseconds(2))) //指定窗口类型
      .apply((black, white) => black._2 + white._2) //应用JoinFunciton
  }

}
