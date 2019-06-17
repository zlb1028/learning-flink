package com.realtime.flink.streaming.sink

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object BasicSink {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val personStream = env.fromElements(("Alex", 18), ("Peter", 43))

    personStream.writeAsCsv("file:///path/to/person.csv", WriteMode.OVERWRITE)

    personStream.writeAsText("file:///path/to/person.txt")

  }
}
