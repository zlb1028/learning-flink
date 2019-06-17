package com.realtime.flink.gelly

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.graph._
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.scala.utils.EdgeToTuple3Map
import org.apache.flink.types.{LongValue, NullValue}


object CsvGraphGeneration {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val graph: Graph[Long, Long, Long] = Graph.fromCollection(getLongLongVertices, getLongLongEdges, env)

    //从CSV文件中创建Graph，同时指定Vertices和Edges路径
    val graph1 = Graph.fromCsvReader[String, Long, Double](pathVertices = "path/vertex_file", pathEdges = "path/edge_file", env = env)

    //从CSV文件中创建Graph，同时指定Vertices和Edges路径
    val graph2 = Graph.fromCsvReader[String, Long, Double](pathVertices = "path/vertex_file", pathEdges = "path/edge_file", env = env)


    //从CSV文件中创建Graph，不指定Vertices路径
    val simpleGraph1 = Graph.fromCsvReader[Long, NullValue, NullValue](pathEdges = "path/to/edge/input", env = env)
    //
    val simpleGraph2 = Graph.fromCsvReader[Long, Double, NullValue](pathEdges = "path/to/edge/input",
      vertexValueInitializer = new MapFunction[Long, Double]() {
        def map(id: Long): Double = {
          id.toDouble
        }
      },
      env = env)

    graph1.filterOnEdges(edge => edge.getValue > 10)

    graph1.filterOnVertices(vertex => vertex.getValue < 10)

    graph1.union(graph2)

    graph1.reverse()

    env.fromCollection(getLongLongVertices)

    graph1.outDegrees()

    val dataSet: DataSet[(Long, Long)] = env.fromElements((182L, 112L), (238L, 34L))

    val outputResult = graph.joinWithEdgesOnSource(dataSet, (v1: Long, v2: Long) => v1 + v2)

    outputResult.getEdges.collect().toList


    val result: Graph[Long, Long, Long] = graph.joinWithEdges(graph.getEdges.map(new EdgeToTuple3Map[Long, Long]), new AddValuesMapper)

    val res = result.getEdges.collect().toList


    val graph3 = Graph.fromCollection(getLongLongVertices, getLongLongEdges1, env)

    val graph4 = Graph.fromCollection(getLongLongVertices, getLongLongEdges2, env)

    //    graph3.getEdges.collect().toList.foreach(println)
    //
    //    graph4.getEdges.collect().toList.foreach(println)

    graph3.difference(graph4).getEdges.collect().toList.foreach(println)

    graph3.addVertex(new Vertex[Long, Long](1, 222))

    graph3.getVertices.collect().toList.foreach(println)

    val maxWeights = graph.reduceOnEdges(new SelectMaxWeightFunction, EdgeDirection.OUT)
    // 定义ReduceEdgesFunction实现对出度边的权重求和
    final class SelectMaxWeightFunction extends ReduceEdgesFunction[Long] {
      override def reduceEdges(firstEdgeValue: Long, secondEdgeValue: Long): Long = {
        Math.max(firstEdgeValue, secondEdgeValue)
      }
    }

    val verticesWithSum = graph.reduceOnNeighbors(new SumValuesFucntion, EdgeDirection.IN)
    // 定义ReduceEdgesFunction实现对每个Target顶点邻近点的和
    final class SumValuesFucntion extends ReduceNeighborsFunction[Long] {
      override def reduceNeighbors(firstNeighbor: Long, secondNeighbor: Long): Long = {
        firstNeighbor + secondNeighbor
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
      new Edge[Long, Long](1L, 2L, 12L),
      new Edge[Long, Long](1L, 3L, 13L),
      new Edge[Long, Long](2L, 3L, 23L),
      new Edge[Long, Long](3L, 4L, 34L),
      new Edge[Long, Long](3L, 5L, 35L),
      new Edge[Long, Long](4L, 5L, 45L),
      new Edge[Long, Long](5L, 1L, 51L)
    )
  }

  def getLongLongEdges1: List[Edge[Long, Long]] = {
    List(
      new Edge[Long, Long](1L, 2L, 12L),
      new Edge[Long, Long](1L, 3L, 13L),
      new Edge[Long, Long](2L, 3L, 23L)
    )
  }

  def getLongLongEdges2: List[Edge[Long, Long]] = {
    List(
      new Edge[Long, Long](1L, 2L, 12L),
      new Edge[Long, Long](1L, 3L, 13L),
      new Edge[Long, Long](1L, 3L, 23L)
    )
  }

  final class AddValuesMapper extends EdgeJoinFunction[Long, Long] {
    @throws(classOf[Exception])
    def edgeJoin(edgeValue: Long, inputValue: Long): Long = {
      edgeValue + inputValue
    }
  }

}
