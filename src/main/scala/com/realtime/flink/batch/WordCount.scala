package com.realtime.flink.batch

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object WordCount {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    val counts = text.flatMap(
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    )
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts.print()

    //    val dataSet: DataSet[Long] = env.fromElements(222, 12, 34, 323)
    //    dataSet.reduceGroup { collector => collector.sum }

    val dataSet: DataSet[(Int, String, Long)] = env.fromElements((12, "Alice", 34), (12, "Alice", 34), (12, "Alice", 34))
    val result: DataSet[(Int, String, Long)] = dataSet.aggregate(Aggregations.SUM, 0).aggregate(Aggregations.MIN, 2)
    val output: DataSet[(Int, String, Long)] = dataSet.sum(0).min(2)

    case class Person(var id: Int, var name: String)
    val dataSet1: DataSet[Person] = env.fromElements(Person(1, "Peter"), Person(2, "Alice"))
    val dataSet2: DataSet[(Double, Int)] = env.fromElements((12.3, 1), (22.3, 3))
    val result2 = dataSet1.join(dataSet2).where("id").equalTo(1)
    val result3 = dataSet1.join(dataSet2).where("id").equalTo(1) {
      (left, right) => (left.id, left.name, right._1 + 1)
    }

    val result4 = dataSet1.join(dataSet2).where("id").equalTo(1) {
      (left, right, collector: Collector[(String, Double, Int)]) =>
        collector.collect(left.name, right._1 + 1, right._2)
        collector.collect("prefix_" + left.name, right._1 + 2, right._2)
    }.withForwardedFieldsFirst("_2->_3")

    dataSet1.join(dataSet2, JoinHint.BROADCAST_HASH_FIRST).where("id").equalTo(1)
    dataSet1.join(dataSet2, JoinHint.BROADCAST_HASH_SECOND).where("id").equalTo(1)
    dataSet1.join(dataSet2, JoinHint.OPTIMIZER_CHOOSES).where("id").equalTo(1)
    dataSet1.join(dataSet2, JoinHint.REPARTITION_HASH_FIRST).where("id").equalTo(1)
    dataSet1.join(dataSet2, JoinHint.REPARTITION_HASH_SECOND).where("id").equalTo(1)
    dataSet1.join(dataSet2, JoinHint.REPARTITION_SORT_MERGE).where("id").equalTo(1)

    dataSet1.leftOuterJoin(dataSet2).where("id").equalTo(1)
    dataSet1.rightOuterJoin(dataSet2).where("id").equalTo(1)

    dataSet1.leftOuterJoin(dataSet2).where("id").equalTo(1) {
      (left, right) =>
        if (right == null) {
          (left.id, 1)
        } else {
          (left.id, right._1)
        }
    }

    dataSet1.leftOuterJoin(dataSet2, JoinHint.BROADCAST_HASH_SECOND).where("id").equalTo(1)

    dataSet1.leftOuterJoin(dataSet2, JoinHint.REPARTITION_HASH_SECOND).where("id").equalTo(1)

    dataSet1.coGroup(dataSet2).where("id").equalTo(1) {
      (left, right, collector: Collector[(Long, String, Double)]) => {
        while (left.hasNext) {
          val person = left.next()

        }
      }
    }.withForwardedFieldsFirst("_1->_2")


    val output2 = dataSet1.coGroup(dataSet2).where("id").equalTo(1) {
      (iVals, dVals, out: Collector[Double]) =>
        val ints = iVals map {
          _.id
        } toSet
        for (dVal <- dVals) {
          for (i <- ints) {
            out.collect(dVal._2 * i)
          }
        }
    }

    val dataSet4: DataSet[String] = env.fromElements("flink")
    val out = dataSet4.rebalance().map {
      _.toUpperCase
    }

    val dataSet5: DataSet[(Int, String)] = env.fromElements((12, "flink"), (22, "spark"))
    val dataSet6: DataSet[String] = env.fromElements("flink")
    val crossDataSet: DataSet[((Int, String), String)] = dataSet5.cross(dataSet6)

  }
}