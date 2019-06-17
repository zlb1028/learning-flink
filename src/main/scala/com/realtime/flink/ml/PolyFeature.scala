package com.realtime.flink.ml

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment,_}
import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.preprocessing.PolynomialFeatures
import org.apache.flink.ml.regression.MultipleLinearRegression

object PolyFeature {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 获取训练数据集
    val trainingDS: DataSet[LabeledVector] = env.fromElements(LabeledVector(1.2,Vector()))
    // 设定多项式转换维度为3
    val polyFeatures = PolynomialFeatures()
      .setDegree(3)
    polyFeatures.fit(trainingDS)
  }

}
