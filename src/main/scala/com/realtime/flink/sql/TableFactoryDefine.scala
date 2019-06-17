package com.realtime.flink.sql

import java.util

import org.apache.flink.table.descriptors.ConnectorDescriptor
import org.apache.flink.table.factories.StreamTableSourceFactory
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row

/**
  * Created by zhanglibing on 2019/3/4
  */
object TableFactoryDefine {

  def main(args: Array[String]): Unit = {

  }

  class SocketTableSourceFactory extends StreamTableSourceFactory[Row] {

    override def requiredContext(): util.Map[String, String] = {
      val context = new util.HashMap[String, String]()
      context.put("update-mode", "append")
      context.put("connector.type", "dev-system")
      context
    }

    override def supportedProperties(): util.List[String] = {
      val properties = new util.ArrayList[String]()
      properties.add("connector.host")
      properties.add("connector.port")
      properties
    }

    override def createStreamTableSource(properties: util.Map[String, String]): StreamTableSource[Row] = {
      val socketHost = properties.get("connector.host")
      val socketPort = properties.get("connector.port")
//      new SocketTableSource(socketHost, socketPort)

    }
  }

  class MySystemConnector(host: String,port:String) extends ConnectorDescriptor("dev-system", 1, false) {
    override protected def toConnectorProperties(): Map[String, String] = {
      val properties = new immutable.HashMap[String, String]
      properties.put("connector.host", host)
      properties.put("connector.port", port)
      properties
    }
  }

}
