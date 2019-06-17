package com.realtime.flink.ml

import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.api.scala._


object Vector {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    LabeledVector(1.0, DenseVector(1))
    val trainCsvData = env.readCsvFile[(String, String, String, String)]("/path/svm_train.data")
    val trainData:DataSet[LabeledVector] = trainCsvData.map { data =>
      val numList = data.productIterator.toList.map(_.asInstanceOf[String].toDouble)
      LabeledVector(numList(3), DenseVector(numList.take(3).toArray))
    }
  }

}
