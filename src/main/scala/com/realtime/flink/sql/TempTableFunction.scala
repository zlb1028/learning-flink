package com.realtime.flink.sql

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import scala.collection.mutable


object TempTableFunction {
  def main(args: Array[String]): Unit = {
    // 首先在代码中配置TableEnvironment
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(sEnv)

    // Create and register an example table using above data set.
    // In the real setup, you should replace this with your own table.
    //    val tempTable = sEnv.fromElements(("id1",122L),("id2",222L),("id3",145L))
    //      .toTable(tEnv, 't_id, 't_, 't_proctime.proctime)
    //
    //    tEnv.registerTable("tempTable", tempTable)

    // Create and register TemporalTableFunction.
    // Define "r_proctime" as the time attribute and "r_currency" as the primary key.
    // <==== (1)
    //    tEnv.registerFunction("tempTable", tempTableFunction) // <==== (2)


    val tempTable = tEnv.scan("TempTable")
    val temps = tempTable.createTemporalTableFunction('t_proctime, 't_id)
    val table = tEnv.scan("Table")
    val result = table.join(temps('o_rowtime), 'table_key == 'temp_key)


    val elements = new String{}
  }
}
