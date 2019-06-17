package com.realtime.flink.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}


object TransformOperation {

  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)
    //创建ExecutionEnvironment环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet: DataSet[String] = env.fromElements("flink", "hadoop", "spark")

    dataSet.map(_.toUpperCase)
    dataSet.map(x => x.toUpperCase)
    val numdataSet: DataSet[Long] = env.fromElements(123, 212, 123, 243)

    numdataSet.filter(x => x > 100)

    numdataSet.reduce((x, y) => x + y)

    val dataSet2: DataSet[Array[Int]] = env.fromElements(Array(3, 9, 1), Array(2, 3, 5))


    val dataSet3: DataSet[(String,Long)] = env.fromElements(("foo",1),("fun",1),("abb",1))



  }
}
