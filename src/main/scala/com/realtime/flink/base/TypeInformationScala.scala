package com.realtime.flink.base

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._


object TypeInformationScala {

  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    //创建类型Int的数据集

    val data: DataSet[String] = env.fromElements("hello", "flink")

    val upperResult = upper(data).print()

  }

  def upper[T](input: DataSet[T]): DataSet[String] = {
    input.map { v => v.toString.toUpperCase }
  }

//  def upper[T: ClassTag](input: DataSet[T])(implicit c: TypeInformation[T]): DataSet[String] = {
//    input.map { v => v.toString.toUpperCase }
//  }


}
