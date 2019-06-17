package com.realtime.flink.batch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.types.Row

object JDBCInputFormatDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customerData: DataSet[Row] =
      env.createInput(
        JDBCInputFormat.buildJDBCInputFormat()
          .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
          .setDBUrl("jdbc:derby:memory:Customer")
          .setQuery("select id, name from persons")
          .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
          .finish()
      )

  }

}
