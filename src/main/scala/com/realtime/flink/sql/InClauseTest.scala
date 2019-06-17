package com.realtime.flink.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._


object InClauseTest {
  def main(args: Array[String]): Unit = {

    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tStreamEnv = TableEnvironment.getTableEnvironment(sEnv)

    val stream1: DataStream[(Long, String)] = sEnv.fromElements((192, "foo"), (122, "fun"))
    val stream2: DataStream[Long] = sEnv.fromElements(92, 122)
    val left: Table = tStreamEnv.fromDataStream(stream1,'id,'name)
    val right: Table = tStreamEnv.fromDataStream(stream2,'id)
    val result1 = left.where('id in(right))
    val result2 = left.where('id in("92","11"))

  }
}
