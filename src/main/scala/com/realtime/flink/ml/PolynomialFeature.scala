package com.realtime.flink.ml

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.preprocessing.PolynomialFeatures
import org.apache.flink.ml.regression.MultipleLinearRegression

/**
  * Created by zhanglibing on 2019/1/15
  */
object PolynomialFeature {

  def main(args: Array[String]): Unit = {
    // Obtain the training data set
    val trainingDS: DataSet[LabeledVector] = null

    // Setup polynomial feature transformer of degree 3
    val polyFeatures = PolynomialFeatures()
      .setDegree(3)

    // Setup the multiple linear regression learner
    val mlr = MultipleLinearRegression()

    // Control the learner via the parameter map
    val parameters = ParameterMap()
      .add(MultipleLinearRegression.Iterations, 20)
      .add(MultipleLinearRegression.Stepsize, 0.5)

    // Create pipeline PolynomialFeatures -> MultipleLinearRegression
    val pipeline = polyFeatures.chainPredictor(mlr)

    // train the model
    pipeline.fit(trainingDS)
  }

}
