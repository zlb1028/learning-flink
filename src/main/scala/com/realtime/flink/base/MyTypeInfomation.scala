package com.realtime.flink.base

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.types.Row

object MyTypeInfomation {

  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    //创建类型Int的数据集
    //    val dataStream: DataStream[Int] = env.fromElements(3, 1, 2, 1, 5)

    val dataStream: DataStream[String] = env.fromElements("hello", "flink")

    //    val dataStream: DataStream[Int] = env.fromCollection(Array(3, 1, 2, 1, 5))

    //    val dataStream: DataStream[Int] = env.fromCollection(List(3, 1, 2, 1, 5))

    //    val dataStream: DataStream[Int] = env.fromCollection(List(3, 1, 2, 1, 5))

    val tupleStream1: DataStream[(String, Int)] = env.fromElements(("a", 1), ("c", 2))

    tupleStream1.keyBy(0).sum(1)

    val tupleStream2: DataStream[Tuple2[String, Int]] = env.fromElements(new Tuple2("a", 1), new Tuple2("c", 2))

    //定义WordCount Case Class数据结构
    case class WordCount(word: String, count: Int)

    //在数据集
    val input = env.fromElements(WordCount("hello", 1), WordCount("world", 2))

    input.keyBy("word") // 根据第一个和第二个字段分区

    val personStream = env.fromElements(new Person("Peter",14),new Person("Linda",25))

    personStream.keyBy("name")


    //    input.print()

    //    tupleStream2.print()


    val mapStream = env.fromElements(Map("name"->"Peter","age"->18),Map("name"->"Linda","age"->25))

    val listStream = env.fromElements(List(1,2,3,5),List(2,4,3,2))

    listStream.print()

//    mapStream.keyBy("name")

    env.execute("Streaming TypeInfomation")

  }


}
