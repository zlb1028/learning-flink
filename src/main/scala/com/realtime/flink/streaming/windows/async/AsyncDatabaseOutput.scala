package com.realtime.flink.streaming.windows.async

import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get, HTable, Table}
import org.apache.hadoop.hbase.util.Bytes
import shapeless.syntax.zipper

import scala.concurrent.ExecutionContext

/**
  * Created by zhanglibing on 2019/2/19
  */
object AsyncDatabaseOutput {

  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建黑色元素数据集
    val blackStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))

    // apply the async I/O transformation
    val resultStream: DataStream[(String, String)] =
      AsyncDataStream.unorderedWait(blackStream, new HBaseAsyncFunction(), 1000, TimeUnit.MILLISECONDS, 100)
  }
}


/**
  * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
  */
class HBaseAsyncFunction extends AsyncFunction[String, String] {

  /** The database specific client that can issue concurrent requests with callbacks */
  val connection: Connection

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  override def asyncInvoke(in: String, resultFuture: ResultFuture[String]): Unit = {
    val htable: Table = connection.getTable(TableName.valueOf(Bytes.toBytes("test")));
    val get = new Get(Bytes.toBytes(in);
    val result = htable.asInstanceOf[AsyncableHTableInterface].asyncGet(get, new UserCallback(c))
    resultFuture.onSuccess {
      case result: String => resultFuture.complete(Iterable((str, result)))
    }
  }







