package com.realtime.flink.sql

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}


object SQLQuery {
  def main(args: Array[String]): Unit = {
    // 首先在代码中配置TableEnvironment
    val bEnv = ExecutionEnvironment.getExecutionEnvironment
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(sEnv)
    // 在TableEnvironment中注册订单表结构
    // ...
    // 指定具体的执行逻辑
    val orders = tEnv.scan("sensors_table") // schema (id, type ,time, var1, var2)

    val result = tEnv.sqlQuery("select sensor_id,sum(var1) from sensors_table where sensor_type='A' group by sensor_id)")


    // get a TableEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(bEnv)

    // 创建CsvTableSink，指定CSV文件地址和切割符
    val csvSink: CsvTableSink = new CsvTableSink("/path/to/file", ",")

    // 定义fieldNames和fieldTypes
    val fieldNames: Array[String] = Array("field1", "field2", "field3")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.DOUBLE, Types.LONG)

    // 将创建的csvSink注册到TableEnvironment中并指定名称为"CsvSinkTable"
    tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)

    val stream: DataStream[(Long, String)] = sEnv.fromElements((12,"foo"),(32,"fun"))
    //
    tEnv.registerDataStream("table1", stream)
    //
    tEnv.registerDataStream("table2", stream, 'id, 'name)


  }

}
