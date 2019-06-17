package com.realtime.flink.streaming.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by zhanglibing on 2019/2/21
  */
object QuerableStateService {
  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建黑色元素数据集
    val inputStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))

    inputStream.keyBy(_._1).flatMap {
      //定义和创建RichFlatMapFunction，第一个参数为输入数据类型，第二参数为输出数据类型
      new RichFlatMapFunction[(Int, Long), (Int, Long, Long)] {

        private var leastValueState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          //创建ValueStateDescriptor，定义状态名称为leastValue，并指定数据类型
          val leastValueStateDescriptor = new ValueStateDescriptor[Long]("leastValue", classOf[Long])
          leastValueStateDescriptor.setQueryable("leastQueryValue")
          //通过getRuntimeContext.getState获取State
          leastValueState = getRuntimeContext.getState(leastValueStateDescriptor)

        }

        override def flatMap(t: (Int, Long), collector: Collector[(Int, Long, Long)]): Unit = {
          //通过value方法从leastValueState中获取最小值
          val leastValue = leastValueState.value()
          //如果当前指标大于最小值，则直接输出数据元素和最小值
          if (t._2 > leastValue) {
            collector.collect((t._1, t._2, leastValue))
          } else {
            //如果当前指标小于最小值，则更新状态中的最小值
            leastValueState.update(t._2)
            //将当前数据中的指标作为最小值输出
            collector.collect((t._1, t._2, t._2))
          }
        }
      }
    }

    val maxInputStream: DataStream[(Int, Long)] = inputStream
      .map(r => (r._1, r._2))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .max(1)
    // 存储每个key在5秒窗口上的最大值
    maxInputStream
      //根据Key进行分区，并转换为可查询状态
      .keyBy(_._1) .asQueryableState("maxInputState")
  }
}

