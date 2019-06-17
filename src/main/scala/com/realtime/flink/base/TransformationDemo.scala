package com.realtime.flink.base

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object TransformationDemo {

  def main(args: Array[String]): Unit = {
    // 输入参数合法性检查
    val params = ParameterTool.fromArgs(args)
    // Streaming执行环境设定
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 让参数能够在web页面中展示
    env.getConfig.setGlobalJobParameters(params)
    val dataStream: DataStream[String] = env.fromElements("hello", "flink")
    dataStream.map(new MyMapFunction)
    dataStream.map(new MapFunction[String, String] {
      //实现对输入字符串大写转换
      override def map(t: String): String = {
        t.toUpperCase()
      }
    })
  }

  class MyMapFunction extends MapFunction[String, String] {
    override def map(t: String): String = {
      t.toUpperCase()
    }
  }

}
