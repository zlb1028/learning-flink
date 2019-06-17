package com.realtime.flink.base

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object MyExecutionEnviroment {

  def main(args: Array[String]): Unit = {
    //设定Flink运行环境，如果在本地启动则执穿件本地环境，如果是在集群上启动，则创建集群环境
    StreamExecutionEnvironment.getExecutionEnvironment
    //指定并行度创建本地执行环境
    StreamExecutionEnvironment.createLocalEnvironment(5)
    //指定远程JobManagerIP和RPC端口
    StreamExecutionEnvironment.createRemoteEnvironment("JobManagerHost",6021,5,"")

    ExecutionEnvironment.getExecutionEnvironment

    ExecutionEnvironment.createLocalEnvironment(5)

    ExecutionEnvironment.createRemoteEnvironment("JobManagerHost",6021,5,"")

  }

}
