package com.realtime.flink.batch

import com.realtime.flink.base.Person
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.types.{Row, StringValue}
import org.apache.hadoop.io.IntWritable

object BatchInputSource {

  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)
    //创建ExecutionEnvironment环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取本地文件
    val textFileDs: DataSet[String] = env.readTextFile("file:///path/textfile")
    //读取HDFS文件
    val hdfsLines: DataSet[String] = env.readTextFile("hdfs://nnHost:nnPort/path/textfile")

    val stringValueDataSet: DataSet[StringValue] = env.readTextFileWithValue("file:///path/textfile", "")

    // read a CSV file with three fields
    val csvInput1: DataSet[(Integer, String, Double)] = env.readCsvFile("hdfs:///the/CSV/file")

    // read a CSV file with five fields, taking only two of them
    val csvInput2: DataSet[(String, Double)] = env.readCsvFile("hdfs:///the/CSV/file", "\n", ",", null, false, null, false, Array(1, 2))

    // read a CSV file with three fields into a POJO (Person.class) with corresponding fields
    val csvInput3: DataSet[Person] = env.readCsvFile("hdfs:///the/CSV/file")


    // read a file from the specified path of type SequenceFileInputFormat
    //    val tuples: DataSet[(IntWritable, Text)] = env.readSequenceFile(IntWritable.class, Text.
    //        class, "hdfs://nnHost:nnPort/path/to/file"
    //        )

    // 从给定元素中创建DataSet数据集
    val elementDataSet: DataSet[String] = env.fromElements("flink", "hadoop", "spark")
    //从集合中创建DataSet数据集
    val collectionDataSet: DataSet[String] = env.fromCollection(Seq("Foo", "bar", "foobar", "fubar"))
    val collectionDataSet2: DataSet[String] = env.fromCollection(Iterable("Foo", "bar", "foobar", "fubar"))

    // generate a number sequence
    val numbers: DataSet[Long] = env.generateSequence(1, 10000000)

    // Read data from a relational database using the JDBC input format
    val dbData: DataSet[Row] =
      env.createInput(
        JDBCInputFormat.buildJDBCInputFormat()
          .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
          .setDBUrl("jdbc:derby:memory:persons")
          .setQuery("select name, age from persons")
          .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
          .finish()
      )


  }

}
