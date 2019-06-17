package com.realtime.flink.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.Tumble
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Created by zhanglibing on 2019/2/26
  */
object TableSample {
  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tStreamEnv = TableEnvironment.getTableEnvironment(sEnv)
    val stream: DataStream[(Long, String)] = sEnv.fromElements((192, "foo"), (122, "fun"))
    // 将DataStream注册成Table，指定表名为table1并使用默认字段名f0,f1
    tStreamEnv.registerDataStream("table1", stream)

    //将DataStream通过fromDataStream转换成Table
    val table1: Table = tStreamEnv.fromDataStream(stream)
    //将DataStream通过fromDataStream转换成Table，并指定字段名称
    // environment configuration
    // ...
    // specify table program
    val orders: Table = tStreamEnv.scan("Sensors") // schema (a, b, c, rowtime)
    val result: Table = orders
      .filter('id.isNotNull && 'var1.isNotNull && 'var2.isNotNull)
      .select('a.lowerCase(), 'b, 'rowtime)
      .window(Tumble over 1.millis on 'rowtime as 'hourlyWindow)
      .groupBy('hourlyWindow, 'a)
      .select('a, 'hourlyWindow.end as 'hour, 'b.avg as 'avgBillingAmount)

    // 通过scan方法在CataLog中找到Sensors表
    val sensors:Table = tStreamEnv.scan("Sensors")
    //对sensors 使用Table API进行处理
    val result2 = sensors
      .groupBy('id)//根据ID进行GroupBy操作
      .select('id, 'var1.sum as 'cnt)//查询id和var1的sum指标
      .toAppendStream[(String,Long)] //将处理结果转换成元祖类型DataStream数据集

    sensors.filter('var1%2 === 0)

  }
}
