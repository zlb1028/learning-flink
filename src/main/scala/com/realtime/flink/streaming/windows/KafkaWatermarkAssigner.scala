package com.realtime.flink.streaming.windows

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

/**
  * Created by zhanglibing on 2019/2/16
  */
object KafkaWatermarkAssigner {

  def main(args: Array[String]): Unit = {

    case class SensorEvent(id: Long, name: String, timestamp: Long)

    val parameterTool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建数组数据集
    //指定系统时间概念为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val kafkaSource = new FlinkKafkaConsumer09[SensorEvent]("ConsumerTopic", new JSONKeyValueDeserializationSchema[SensorEvent](false), parameterTool.getProperties)
//
//    kafkaSource.assignTimestampsAndWatermarks(t => t.)
//
//    val stream: DataStream[(String, Long, Int)] = env.addSource(kafkaSource)


  }


}
