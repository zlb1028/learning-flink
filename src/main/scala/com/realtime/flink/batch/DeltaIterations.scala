package com.realtime.flink.batch

import org.apache.flink.api.scala.DataSet

/**
  * Created by zhanglibing on 2019/3/13
  */
class DeltaIterations {
  def main(args: Array[String]): Unit = {
    // read the initial data sets
    val initialSolutionSet: DataSet[(Long, Double)] = // [...]

    val initialWorkset: DataSet[(Long, Double)] = // [...]

    val maxIterations = 100
    val keyPosition = 0

    val result = initialSolutionSet.iterateDelta(initialWorkset, maxIterations, Array(keyPosition)) {
      (solution, workset) =>
        val candidateUpdates = workset.groupBy(1).reduceGroup(new ComputeCandidateChanges())
        val deltas = candidateUpdates.join(solution).where(0).equalTo(0)(new CompareChangesToCurrent())

        val nextWorkset = deltas.filter(new FilterByThreshold())

        (deltas, nextWorkset)
    }

    result.writeAsCsv(outputPath)

    env.execute()
  }

}
