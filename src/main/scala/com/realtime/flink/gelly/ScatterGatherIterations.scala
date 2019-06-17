package com.realtime.flink.gelly

import com.realtime.flink.gelly.VertexCentricIterations.{getLongLongEdges, getLongLongVertices}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction, ScatterGatherConfiguration}

object ScatterGatherIterations {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val graph: Graph[Long, Long, Long] = Graph.fromCollection(getLongLongVertices, getLongLongEdges, env)

    val config = new ScatterGatherConfiguration
    // define the maximum number of iterations
    val maxIterations = 10

    // Execute the scatter-gather iteration
    val result = graph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater,maxIterations ,config)

    // Extract the vertices as the result
    val singleSourceShortestPaths = result.getVertices

    // - - -  UDFs - - - //

    // 定义ScatterFunction
    final class MinDistanceMessenger extends ScatterFunction[Long, Long, Long, Long] {

      override def sendMessages(vertex: Vertex[Long, Long]) = {
        for (edge: Edge[Long, Long] <- getEdges) {
          sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
        }
      }
    }

    // 定义GatherFunction更新顶点指标
    final class VertexDistanceUpdater extends GatherFunction[Long, Long, Long] {


      override def updateVertex(vertex: Vertex[Long, Long], inMessages: MessageIterator[Long]) = {

        var minDistance = Long.MaxValue

        while (inMessages.hasNext) {
          val msg = inMessages.next
          if (msg < minDistance) {
            minDistance = msg
          }
        }

        if (vertex.getValue > minDistance) {
          setNewVertexValue(minDistance)
        }
      }
    }
  }


}
