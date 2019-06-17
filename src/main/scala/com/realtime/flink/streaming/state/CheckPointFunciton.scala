package com.realtime.flink.streaming.state

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by zhanglibing on 2019/2/21
  */
object CheckPointFunciton {


  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建黑色元素数据集
    val inputStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))

  }

}

case class CustomCaseClass(id: String, temperature: Long)

private class CheckpointCount(val numElements: Int)
  extends FlatMapFunction[(Int, Long), (Int, Long, Long)] with CheckpointedFunction {
  //定义算子实例本地变量，存储Operator数据数量
  private var operatorCount: Long = _
  //定义keyedState，存储和Key相关的状态值
  private var keyedState: ValueState[Long] = _
  //定义operatorState，存储算子的状态值
  private var operatorState: ListState[Long] = _

  override def flatMap(t: (Int, Long), collector: Collector[(Int, Long, Long)]): Unit = {

    val keyedCount = keyedState.value() + 1
    //更新keyedState数量
    keyedState.update(keyedCount)
    //更新本地算子operatorCount值
    operatorCount = operatorCount + 1
    //输出结果，包括id，id对应的数量统计keyedCount，算子输入数据的数量统计operatorCount
    collector.collect((t._1, keyedCount, operatorCount))
  }

  //初始化状态数据
  override def initializeState(context: FunctionInitializationContext): Unit = {
    //定义并获取keyedState
    keyedState = context.getKeyedStateStore.getState(
      new ValueStateDescriptor[Long](
        "keyedState", createTypeInformation[Long]))

    //定义并获取operatorState
    operatorState = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[Long](
        "operatorState", createTypeInformation[Long]))
    //定义在Restored过程中，从operatorState中恢复数据的逻辑
    if (context.isRestored) {
      operatorCount = operatorState.get().asScala.sum
    }
  }
    //当发生snapshot时，将operatorCount添加到operatorState中
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    operatorState.clear()
    operatorState.add(operatorCount)
  }
}
