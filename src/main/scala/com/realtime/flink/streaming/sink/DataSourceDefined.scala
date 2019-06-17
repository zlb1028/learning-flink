package com.realtime.flink.streaming.sink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path

object DataSourceDefined {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val wordStream = env.fromElements("Alex", "Peter", "Linda")

    //直接读取文本文件
    val textStream = env.readTextFile("/user/local/data_example.log")
    //通过指定CSVInputFormat读取CSV文件
    val csvStream = env.readFile(new CsvInputFormat[String](new Path("/user/local/data_example.csv")) {
      override def fillRecord(out: String, objects: Array[AnyRef]): String = {
        return null
      }
    }, "/user/local/data_example.csv")

    val socketDataStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    val dataStream = env.fromElements(Tuple2(1L, 3L), Tuple2(1L, 5L), Tuple2(1L, 7L), Tuple2(1L, 4L), Tuple2(1L, 2L))

  }

}
