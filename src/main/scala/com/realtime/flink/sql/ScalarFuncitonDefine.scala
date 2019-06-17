package com.realtime.flink.sql

import com.realtime.flink.sql.EventTimeTableSourceDefine.InputEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.api.scala._

object ScalarFuncitonDefine {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tStreamEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 注册输入数据源
    tStreamEnv.registerTableSource("InputTable", new InputEventSource)
    //在窗口中使用输入数据源，并基于TableSource中定义的EventTime字段创建窗口
    val table: Table = tStreamEnv.scan("InputTable")
    // 在Object或者静态环境中创建自定义函数
    class Add extends ScalarFunction {
      def eval(a: Int, b: Int): Int = {
        if (a == null || b == null) null
        a + b
      }

      def eval(a: Double, b: Double): Double = {
        if (a == null || b == null) null
        a + b
      }
    }
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    // 在Scala Table API中使用自定义函数
    val add = new Add
    val result = table.select('a, 'b, add('a, 'b))
    // 在Table Environment中注册自定义函数，并在SQL中使用
    tableEnv.registerFunction("add", new Add)
    tableEnv.sqlQuery("SELECT a,b, ADD(a,b) FROM InputTable")
  }

}
