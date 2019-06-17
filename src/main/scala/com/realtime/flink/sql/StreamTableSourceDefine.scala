package com.realtime.flink.sql


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.{TimeCharacteristic, datastream, environment}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment, TableSchema, Types}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row


object StreamTableSourceDefine {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tStreamEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 注册输入数据源
    tStreamEnv.registerTableSource("InputTable", new InputEventSource)
    //在窗口中使用输入数据源，并基于TableSource中定义的EventTime字段创建窗口
    val table: Table = tStreamEnv.scan("InputTable") //Schema:origin

    // 定义InputEventSource
    class InputEventSource extends StreamTableSource[Row] {

      override def getReturnType = {
        val names = Array[String]("id", "value")
        val types = Array[TypeInformation[_]](Types.STRING, Types.LONG)
        Types.ROW(names, types)
      }

//      override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
//        // 定义获取DataStream数据集
////        val inputStream: DataStream[(String, Long)] = execEnv.addSource(...)
//        //将数据集转换成指定数据类型
////        val stream: DataStream[Row] = inputStream.map(t => Row.of(t._1, t._2))
////        stream
//        null
//      }


      override def getDataStream(execEnv: environment.StreamExecutionEnvironment): datastream.DataStream[Row] = ???

      //定义TableSchema信息
      override def getTableSchema: TableSchema = {
        val names = Array[String]("id", "value")
        val types = Array[TypeInformation[_]](Types.STRING, Types.LONG)
        new TableSchema(names, types)
      }
    }
  }
}
