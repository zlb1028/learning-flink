package com.realtime.flink.sql

import com.realtime.flink.sql.EventTimeTableSourceDefine.InputEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.api.scala._

object TableFunctionDefine {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tStreamEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 注册输入数据源
    tStreamEnv.registerTableSource("InputTable", new InputEventSource)
    //在窗口中使用输入数据源，并基于TableSource中定义的EventTime字段创建窗口
    val table: Table = tStreamEnv.scan("InputTable")//Schema:origin

    // 在Scala Table API中使用自定义函数
    val split = new SplitFunction(",")
    //在join函数中调用Table Function，将string字符串切分成不同的Row,并通过as指定字段名称为str,length,hashcode
    table.join(split('origin as('string, 'length, 'hashcode))).select('origin, 'str, 'length, 'hashcode)
    table.leftOuterJoin(split('origin as('string, 'length, 'hashcode))).select('origin, 'str, 'length, 'hashcode)
    // 在Table Environment中注册自定义函数，并在SQL中使用
    tStreamEnv.registerFunction("split", new SplitFunction(","))
    //在SQL中和LATERAL TABLE一起使用Table Function
    //和Table API的JOIN一样，产生笛卡尔积结果
    tStreamEnv.sqlQuery("SELECT origin, str, length FROM InputTable, LATERAL TABLE(split(origin)) as T(str, length,hashcode)")
    //和Table API中的LEFT OUTER JOIN一样，产生左外关联结果
    tStreamEnv.sqlQuery("SELECT origin, str, length FROM InputTable, LATERAL TABLE(split(origin)) as T(str, length,hashcode) ON TRUE")
  }

  class SplitFunction(separator: String) extends TableFunction[(String, Int, Int)] {
    def eval(str: String): Unit = {
      str.split(separator).foreach(x => collect((x, x.length, x.hashCode)))
    }
  }

}
