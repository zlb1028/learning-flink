package com.realtime.flink.streaming.state

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector


object ListCheckPoint {

}

class numberRecordsCount extends FlatMapFunction[(String, Long), (String, Long)] with ListCheckpointed[Long] {
  //定义算子中接入的numberRecords数量
  private var numberRecords: Long = 0L

  override def flatMap(t: (String, Long), collector: Collector[(String, Long)]): Unit = {
    //进入一条记录则进行统计，并输出
    numberRecords += 1
    collector.collect(t._1, numberRecords)

  }

  override def snapshotState(checkpointId: Long, ts: Long): util.List[Long] = {
    //Snapshot状态的过程中将numberRecords写入
    Collections.singletonList(numberRecords)
  }

  override def restoreState(list: util.List[Long]): Unit = {
//    numberRecords = 0L
//    for (count <- list) {
//      //通过从状态中恢复numberRecords数据
//      numberRecords += count
//    }
  }
}
