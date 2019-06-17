package com.realtime.flink.sql

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._

object DataStreamWithTable {
  def main(args: Array[String]): Unit = {
    // 首先在代码中配置TableEnvironment
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tStreamEnv = TableEnvironment.getTableEnvironment(sEnv)
    val stream: DataStream[(Long, String)] = sEnv.fromElements((192, "foo"), (122, "fun"))
    // 将DataStream注册成Table，指定表名为table1并使用默认字段名f0,f1
    tStreamEnv.registerDataStream("table1", stream)
    // 将DataStream注册成Table，指定表名为table2和字段名称为field1, field2
    tStreamEnv.registerDataStream("table2", stream, 'field1, 'field2)

    //将DataStream通过fromDataStream转换成Table
    val table1: Table = tStreamEnv.fromDataStream(stream)
    //将DataStream通过fromDataStream转换成Table，并指定字段名称
    val table2: Table = tStreamEnv.fromDataStream(stream, 'field1, 'field2)

    val bEnv = ExecutionEnvironment.getExecutionEnvironment

    val table: Table = tStreamEnv.fromDataStream(stream)

    // 将table通过toAppendStream方法转换成Row格式的DataStream
    val dsRow: DataStream[Row] = tStreamEnv.toAppendStream[Row](table)
    // 将table通过toAppendStream方法转换成Tuple2[String, Int]格式的DataStream
    val dsTuple: DataStream[(Long, String)] = tStreamEnv.toAppendStream[(Long, String)](table)
    // convert the Table into a retract DataStream of Row.
    //   A retract stream of type X is a DataStream[(Boolean, X)].
    //   The boolean field indicates the type of the change.
    //   True is INSERT, false is DELETE.
    // 将table通过toRetractStream方法转换成Row格式的DataStream，返回结果类型为(Boolean, Row)
    // 根据第一个字段是否为True确认是插入数据还是删除数据
    val retractStream: DataStream[(Boolean, Row)] = tStreamEnv.toRetractStream[Row](table)


    // 将DataStream转换成Table，并且使用默认的字段名称"_1","_2"
    val table3: Table = tStreamEnv.fromDataStream(stream)
    //将DataStream转换成Table，并且使用field1,field2作为Table字段名称
    val table4: Table = tStreamEnv.fromDataStream(stream, 'field1, 'field2)


//    // 将DataStream转换成Table，并且使用默认的字段名称"_1","_2"
//    val table: Table = tStreamEnv.fromDataStream(stream)
//    // 将DataStream转换成Table，并且仅获取字段名称"_2"的字段
//    val table: Table = tStreamEnv.fromDataStream(stream, '_2)
//    // 将DataStream转换成Table，并且交换两个字段的位置
//    val table: Table = tStreamEnv.fromDataStream(stream, '_2, '_1)
//    // 将DataStream转换成Table，并且交换两个字段的位置，分别对两个字段进行重命名
//    val table: Table = tStreamEnv.fromDataStream(stream, '_2 as 'field1, '_1 as 'field2)
//
//
//    // 定义Event Case Class
//    case class Event(id: String, time: Long, variable: Int)
//    // 将DataStream转换成Table，并且使用默认字段名称id,time,variable
//    val table = tStreamEnv.fromDataStream(stream)
//    // 将DataStream转换成Table，并且基于位置重新指定字段名称为"field1", "field2", "field3"
//    val table = tStreamEnv.fromDataStream(stream, 'field1, 'field2, 'field3)
//    // 将DataStream转换成Table，
//    val table: Table = tStreamEnv.fromDataStream(stream, 'time as 'newTime, 'id as 'newId,'variable as 'newVariable)

  }
}
