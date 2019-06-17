package com.realtime.flink.gelly

import com.realtime.flink.gelly.VertexCentricIterations.{getLongLongEdges, getLongLongVertices}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.graph.generator.CompleteGraph
import org.apache.flink.graph.gsa._
import org.apache.flink.graph.scala.Graph

object ScatterGatherApplyIterations {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 初始化Graph
    val graph: Graph[Long, Long, Long] = Graph.fromCollection(getLongLongVertices, getLongLongEdges, env)
    // 定义GSAConfiguration
    val config = new GSAConfiguration
    // 定义最大迭代次数
    val maxIterations = 10
    //    val graph2 = new CompleteGraph(env.getJavaEnv, 1).generate()
    // 执行 GSA iteration
    val result = graph.runGatherSumApplyIteration(new CalculateDistances, new ChooseMinDistance, new UpdateDistance, maxIterations, config)

    // 抽取执行结果
    val singleSourceShortestPaths = result.getVertices

    // 定义GatherFunction，计算距离
    final class CalculateDistances extends GatherFunction[Long, Long, Long] {
      override def gather(neighbor: Neighbor[Long, Long]): Long = {
        neighbor.getNeighborValue + neighbor.getEdgeValue
      }
    }

    // 定义SumFunction
    final class ChooseMinDistance extends SumFunction[Long, Long, Long] {
      override def sum(newValue: Long, currentValue: Long): Long = {
        Math.min(newValue, currentValue)
      }
    }

    // 定义ApplyFunction
    final class UpdateDistance extends ApplyFunction[Long, Long, Long] {
      override def apply(newDistance: Long, oldDistance: Long) = {
        if (newDistance < oldDistance) {
          setResult(newDistance)
        }
      }
    }


  }

}
