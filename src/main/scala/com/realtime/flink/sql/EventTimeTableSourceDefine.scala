package com.realtime.flink.sql

import java.util.Collections

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.api.scala.Tumble
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._


object EventTimeTableSourceDefine {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tStreamEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 注册输入数据源
    tStreamEnv.registerTableSource("InputEvent", new InputEventSource)
    //在窗口中使用输入数据源，并基于TableSource中定义的EventTime字段创建窗口
    val windowedTable = tStreamEnv
      .scan("InputEvent")
      .window(Tumble over 10.minutes on 'event_time as 'window)
  }

  // 定义InputEventSource，并实现DefinedRowtimeAttributes接口定义Event Time时间字段信息
  class InputEventSource extends StreamTableSource[Row] with DefinedRowtimeAttributes {

    override def getReturnType = {
      val names = Array[String]("id", "value", "event_time")
      val types = Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.LONG)
      Types.ROW(names, types)
    }

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
      // 定义获取DataStream数据集的逻辑
      val inputStream: DataStream[(String, String, Long)] = null
      //       ...
      //       指定数据集中的EventTime时间信息和Watermark
      val stream = inputStream.assignTimestampsAndWatermarks(...)
      stream
    }

    //定义Table API中的时间字段信息
    override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
      // 创建基于event_time的RowtimeAttributeDescriptor，确定时间字段信息
      val rowtimeAttrDescr = new RowtimeAttributeDescriptor(
        "event_time",
        new ExistingField("event_time"),
        new AscendingTimestamps)
      val rowtimeAttrDescrList = Collections.singletonList(rowtimeAttrDescr)
      rowtimeAttrDescrList
    }
  }


}
