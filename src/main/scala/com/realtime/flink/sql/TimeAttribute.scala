package com.realtime.flink.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.Tumble
import org.apache.flink.table.api.scala._

object TimeAttribute {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tStreamEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    创建元素数据集
    val inputStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))

    val stream: DataStream[(Int, Long)] = inputStream.assignAscendingTimestamps(t => t._2)

    // declare an additional logical field as an event time attribute
//    val table = tStreamEnv.fromDataStream(stream, 'Username, 'Data, 'UserActionTime.rowtime)
    // extract timestamp from first field, and assign watermarks based on knowledge of the stream
//    val stream2: DataStream[(Long, String, String)] = inputStream.assignAscendingTimestamps(t => t._1)

    // the first field has been used for timestamp extraction, and is no longer necessary
    // replace first field with a logical event time attribute
    val table = tStreamEnv.fromDataStream(stream, 'UserActionTime.rowtime, 'Username, 'Data)

    // Usage:

    val windowedTable = table.window(Tumble over 10.minutes on 'UserActionTime as 'userActionWindow)

  }
}
