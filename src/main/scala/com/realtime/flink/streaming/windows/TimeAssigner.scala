package com.realtime.flink.streaming.windows

import breeze.linalg.max
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by zhanglibing on 2019/2/16
  */
object TimeAssigner {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建数组数据集
    //指定系统时间概念为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input = env.fromCollection(List(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1)))
    //使用系统默认Ascending分配时间信息和Watermark
    val withTimestampsAndWatermarks1 = input.assignAscendingTimestamps(t => t._3)

    val withTimestampsAndWatermarks2 = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(10)) {
      override def extractTimestamp(t: (String, Long, Int)): Long = t._2
    })

    //对数据集进行窗口运算
    val result = withTimestampsAndWatermarks1.keyBy(0).timeWindow(Time.seconds(10)).sum("_2")

    env.execute("TimeAssigner App")
  }

  class PeriodicAssigner extends AssignerWithPeriodicWatermarks[(String, Long, Int)] {
    val maxOutOfOrderness = 1000L // 1秒时延设定，表示在1秒以内的数据延时可以计算，超过一秒的数据被认定为迟到事件
    var currentMaxTimestamp: Long = _

    override def extractTimestamp(event: (String, Long, Int), previousEventTimestamp: Long): Long = {
      //获取当前事件时间
      val currentTimestamp = event._2
      //对比当前的事件时间和历史最大事件时间，选择最新的事件时间并赋值给currentMaxTimestamp变量
      currentMaxTimestamp = max(currentTimestamp, currentMaxTimestamp)
      currentTimestamp
    }

    override def getCurrentWatermark(): Watermark = {
      // 根据最大事件时间减去最大的乱序时延长度，然后得到Watermark
      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }
  }

  class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[(String, Long, Int)] {

    val maxTimeLag = 5000L // 5 seconds

    override def extractTimestamp(element: (String, Long, Int), previousElementTimestamp: Long): Long = {
      element._2
    }

    override def getCurrentWatermark(): Watermark = {
      // 返回Watermark为当前系统时间减去最大的时间延时
      new Watermark(System.currentTimeMillis() - maxTimeLag)
    }
  }

  class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[(String, Long, Int)] {
    //复写extractTimestamp方法，定义抽取Timestamp逻辑
    override def extractTimestamp(element: (String, Long, Int), previousElementTimestamp: Long): Long = {
      element._2
    }
    //复写checkAndGetNextWatermark方法，定义Watermark生成逻辑
    override def checkAndGetNextWatermark(lastElement: (String, Long, Int), extractedTimestamp: Long): Watermark = {
      //根据元素中第三位字段状态是否为0生成Watermark
      if (lastElement._3 == 0) new Watermark(extractedTimestamp) else null
    }
  }

}
