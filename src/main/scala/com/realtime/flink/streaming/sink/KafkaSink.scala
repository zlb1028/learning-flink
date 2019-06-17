package com.realtime.flink.streaming.sink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaSink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val wordStream = env.fromElements("Alex", "Peter", "Linda")
    //定义FlinkKafkaProducer011 Sink算子
    val kafkaProducer = new FlinkKafkaProducer011[String](
      "localhost:9092", // 指定Broker List参数
      "kafka-topic", // 指定目标Kafka Topic名称
      new SimpleStringSchema) // 设定序列化Schema
    //通过addsink添加kafkaProducer到算子拓扑中
    wordStream.addSink(kafkaProducer)

    env.execute("KafkaSink")
  }

}
