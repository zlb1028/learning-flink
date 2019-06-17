package com.realtime.flink.gelly

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.graph.scala.Graph

object VertexCentricIterations {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val graph: Graph[Long, Long, Long] = Graph.fromCollection(getLongLongVertices, getLongLongEdges, env)

    // 定义最大迭代次数
    val maxIterations = 10

    // 执行vertex-centric iteration
    val result = graph.runVertexCentricIteration(new ShortestPathComputeFunction, new ShortestPathMsgCombiner, maxIterations)

    // 提取最短路径
    val shortestPaths = result.getVertices

    val srcId = 1L

    //定义ComputeFunction
    final class ShortestPathComputeFunction extends ComputeFunction[Long, Long, Long, Long] {

      override def compute(vertex: Vertex[Long, Long], messages: MessageIterator[Long]) = {

        var minDistance = if (vertex.getId.equals(srcId)) 0 else Long.MaxValue

        while (messages.hasNext) {
          val msg = messages.next
          if (msg < minDistance) minDistance = msg
        }

        if (vertex.getValue > minDistance) {
          setNewVertexValue(minDistance)
          getEdges.forEach(edge => sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue))
        }
      }
    }

    // 定义Message Combiner，合并Message消息
    final class ShortestPathMsgCombiner extends MessageCombiner[Long, Long] {

      override def combineMessages(messages: MessageIterator[Long]) {

        var minDistance = Long.MaxValue

        while (messages.hasNext) {
          val message = messages.next
          if (message < minDistance) {
            minDistance = message
          }
        }
        sendCombinedMessage(minDistance)
      }
    }

  }

  def getLongLongVertices: List[Vertex[Long, Long]] = {
    List(
      new Vertex[Long, Long](1L, 1L),
      new Vertex[Long, Long](2L, 2L),
      new Vertex[Long, Long](3L, 3L),
      new Vertex[Long, Long](4L, 4L),
      new Vertex[Long, Long](5L, 5L)
    )
  }

  def getLongLongEdges: List[Edge[Long, Long]] = {
    List(
      new Edge[Long, Long](1L, 2L, 12),
      new Edge[Long, Long](1L, 3L, 13L),
      new Edge[Long, Long](2L, 3L, 23L),
      new Edge[Long, Long](3L, 4L, 34L),
      new Edge[Long, Long](3L, 5L, 35L),
      new Edge[Long, Long](4L, 5L, 45L),
      new Edge[Long, Long](5L, 1L, 51L)
    )
  }


}
