package com.realtime.flink.streaming.windows

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * Created by zhanglibing on 2019/2/16
  */
object EventTimeAndWatermark {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建数组数据集
    val input = List(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1))
    //添加DataSource数据源，实例化SourceFunction接口
    val source: DataStream[(String, Long, Int)] = env.addSource(
      new SourceFunction[(String, Long, Int)]() {
        //复写run方法，调用SourceContext接口
        override def run(ctx: SourceContext[(String, Long, Int)]): Unit = {
          input.foreach(value => {
            //调用collectWithTimestamp增加Event Time抽取
            ctx.collectWithTimestamp(value, value._2)
            //调用emitWatermark，创建Watermark，最大延时设定为1
            ctx.emitWatermark(new Watermark(value._2 - 1))
          })
          //设定默认Watermark
          ctx.emitWatermark(new Watermark(Long.MaxValue))
        }
        override def cancel(): Unit = {}

      })
  }

}
