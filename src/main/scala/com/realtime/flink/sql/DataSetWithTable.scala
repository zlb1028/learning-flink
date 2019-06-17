package com.realtime.flink.sql

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object DataSetWithTable {
  def main(args: Array[String]): Unit = {
    // 首先在代码中配置TableEnvironment

    val bEnv = ExecutionEnvironment.getExecutionEnvironment

    val tBatchEnv = TableEnvironment.getTableEnvironment(bEnv)

    val dataSet: DataSet[(Long, String)] = bEnv.fromElements((192, "foo"), (122, "fun"))
    // 将DataSet注册成Table，指定表名为table1并使用默认字段名f0,f1
    tBatchEnv.registerDataSet("table1", dataSet)
    // 将DataSet注册成Table，指定表名为table2和字段名称为field1, field2
    tBatchEnv.registerDataSet("table2", dataSet, 'field1, 'field2)

    //将DataStream通过fromDataStream转换成Table
    val table1 = tBatchEnv.fromDataSet(dataSet)
    //将DataStream通过fromDataStream转换成Table，并指定字段名称
    val table2 = tBatchEnv.fromDataSet(dataSet, 'field1, 'field2)

    // Table with two fields (String name, Integer age)
    val table: Table = tBatchEnv.fromDataSet(dataSet)
    // 将Table转换成Row数据类型DataSet数据集
    val rowDS: DataSet[Row] = tBatchEnv.toDataSet[Row](table)
    //将Table转换成Tuple2(Long,String)类型数据类型DataSet数据集
    val tupleDS: DataSet[(Long, String)] = tBatchEnv.toDataSet[(Long, String)](table)

  }
}
