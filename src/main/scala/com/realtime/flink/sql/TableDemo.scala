package com.realtime.flink.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

object TableDemo {
  def main(args: Array[String]): Unit = {

    // 首先在代码中配置TableEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    // 在TableEnvironment中注册订单表结构
    // ...
    // 指定具体的执行逻辑
    val orders = tEnv.scan("speed_sensors") // schema (id, time, speed)
    val result = orders
      .groupBy('id)
      .select('id, 'speed.sum/'ss.count as 'cnt)
      .toDataSet[Row] // 将Table转换成DataSet数据集
      .print()

  }
}
