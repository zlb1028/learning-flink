package com.realtime.flink.batch

import org.apache.calcite.jdbc.CalcitePrepare.Dummy
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.streaming.api.datastream.KeyedStream

object GroupTransform {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("hello", 1), ("flink", 3))
    //根据第一个字段进行数据重分区
    val groupedDataSet: GroupedDataSet[(String, Int)] = dataSet.groupBy(0)
    //求取相同key值下第二个字段的最大值
    groupedDataSet.max(1)

    case class Persion(name: String, age: Int)

    val personDataSet = env.fromElements(new Persion("Alex", 18), new Persion("Peter", 43))

    personDataSet.groupBy("name").max(1)

    class Person(complex: ComplexNestedClass, var count: Int) {
      def this() {
        this(null, 0)
      }
    }

    class ComplexNestedClass(
                              var someNumber: Int,
                              someFloat: Float,
                              word: (Long, Long, String),
                              hadoopCitizen: Int) {
      def this() {
        this(0, 0, (0, 0, ""), 1)
      }
    }

    import org.apache.flink.api.java.functions.KeySelector

    case class WC(word: String, count: Int)

    val words = env.fromElements(WC("hello",1),WC("flink",4))
//    val keyed: KeyedStream[WC]= words.keyBy(new KeySelector[WC, String]() {
//      override def getKey(wc: WC): String = wc.word
//    })

    env.execute()
  }

}
