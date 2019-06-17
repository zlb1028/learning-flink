package com.realtime.flink.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, TableSchema, Types}
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource}
import org.apache.flink.types.Row


object BatchTableSourceDefine {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tBatchEnv = TableEnvironment.getTableEnvironment(env)
    // 注册输入数据源
    tBatchEnv.registerTableSource("InputBatchTable", new InputBatchSource)
    //在窗口中使用输入数据源，并基于TableSource中定义的EventTime字段创建窗口
    val table: Table = tBatchEnv.scan("InputTable") //Schema:origin

    // 定义InputEventSource
    class InputBatchSource extends BatchTableSource[Row] {

      override def getReturnType = {
        val names = Array[String]("id", "value")
        val types = Array[TypeInformation[_]](Types.STRING, Types.LONG)
        Types.ROW(names, types)
      }

      //获取DataSet数据集
      override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
        //从外部系统中读取数据
        //                val inputDataSet = execEnv.createInput(null)
        //
        //                val dataSet: DataSet[Row] = inputDataSet.map(t => Row.of(t._1, t._2))
        //                dataSet
        null
      }

      //定义TableSchema信息
      override def getTableSchema: TableSchema = {
        val names = Array[String]("id", "value")
        val types = Array[TypeInformation[_]](Types.STRING, Types.LONG)
        new TableSchema(names, types)
      }
    }
  }
}
