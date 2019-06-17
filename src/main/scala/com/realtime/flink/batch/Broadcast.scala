package com.realtime.flink.batch

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration


/**
  * Created by zhanglibing on 2019/3/13
  */
object Broadcast {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 创建需要广播的数据集
    val dataSet1: DataSet[Int] = env.fromElements(1, 2, 3, 4)
    //创建输入数据集
    val dataSet2: DataSet[String] = env.fromElements("flink", "dddd")
    dataSet2.map(new RichMapFunction[String, String]() {
      var broadcastSet: Traversable[Int] = null

      override def open(config: Configuration): Unit = {
        // 获取广播变量数据集，并且转换成Collection对象
        broadcastSet = getRuntimeContext().getBroadcastVariable[Int]("broadcastSet-1").asScala
      }

      def map(input: String): String = {
        input + broadcastSet.toList
      }

      //广播DataSet数据集，指定广播变量名称为broadcastSetName
    }).withBroadcastSet(dataSet1, "broadcastSet-1")

  }

  // 定义RichMapFunction获取分布式缓存文件
  class Mapper extends RichMapFunction[String, Int] {

    var myFile: File = null

    override def open(config: Configuration): Unit = {

      //  通过RuntimeContext和DistributedCache获取缓存文件
      myFile = getRuntimeContext.getDistributedCache.getFile("hdfsFile")

    }

    override def map(value: String): Int = {
      // 使用读取得到的文件内容
      val inputFile = new FileInputStream(myFile)
      inputFile.read()
    }
  }

  @ForwardedFields("_1->_2")
  class MyMap extends MapFunction[(Int, Double), (Double, Int)] {
    def map(t: (Int, Double)): (Double, Int) = {
      return (t._2 / 2, t._1)
    }
  }




}
