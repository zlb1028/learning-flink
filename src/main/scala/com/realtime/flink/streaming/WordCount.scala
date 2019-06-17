package com.realtime.flink.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}


object WordCount {

  def main(args: Array[String]) {

    // 输入参数合法性检查
    val params = ParameterTool.fromArgs(args)

    // Streaming执行环境设定
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 让参数能够在web页面中展示
    env.getConfig.setGlobalJobParameters(params)

    // 通过指定数据源地址获取输入数据
    val text = env.readTextFile(params.get("input"))
    // 转换操作
    val counts: DataStream[(String, Int)] = text
      // 将每行数据按照空格切割并且转换成keyalue结构:(word,1)
      .flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      // 通过word名称对数据中的单词次数进行聚合，得出最后的单词出现次数
      .keyBy(0)
      .sum(1)

    // 统计结果输出到文件系统或者直接客户端打印输出
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // 执行流式任务并且指定名称
    env.execute("Streaming WordCount")
  }
}
