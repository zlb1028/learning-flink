package com.realtime.flink.gelly

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.graph.scala.Graph

object GraphCreation {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //创建顶点
    val vertices: List[Vertex[Long, Long]] = List[Vertex[Long, Long]](
      new Vertex[Long, Long](6L, 6L)
    )
    //创建边
    val edges: List[Edge[Long, Long]] = List[Edge[Long, Long]](
      new Edge[Long, Long](6L, 1L, 61L)
    )
    //创建图
    val graph = Graph.fromCollection(vertices, edges, env)

  }
}
