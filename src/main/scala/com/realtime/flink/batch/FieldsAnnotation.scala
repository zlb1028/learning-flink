package com.realtime.flink.batch

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.FunctionAnnotation.{NonForwardedFields, ReadFields}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object FieldsAnnotation {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet1 = env.fromElements(("foo", 12), ("fun", 22))
    val dataSet2 = env.fromElements((12, 124), (23, 223))

    @NonForwardedFields("_2;_3")
    class MyMapper1 extends MapFunction[(String, Long, Int), (String, Long, Int)] {
      def map(input: (String, Long, Int)): (String, Long, Int) = {
        return (input._1, input._2 / 2, input._3 * 4)
      }
    }

    @ReadFields("_1; _2")
    class MyMapper extends MapFunction[(Int, Int, Double, Int), (Int, Long)]{
      def map(value: (Int, Int, Double, Int)): (Int, Double) = {
        if (value._1 == 42) {
          return (value._1, value._3)
        } else {
          return (value._2 + 10, value._3)
        }
      }
    }

  }

}
