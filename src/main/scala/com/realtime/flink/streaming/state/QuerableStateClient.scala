package com.realtime.flink.streaming.state

import java.util.concurrent.CompletableFuture

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation, Types}
import org.apache.flink.queryablestate.client.QueryableStateClient

object QuerableStateClient {

  def main(args: Array[String]): Unit = {
    val tmHostname: String = "localhost"
    val proxyPort: Int = 9069
    val jobId: String = "d1227b1a350d952c372re4c886d2re243"
    val key: Integer = 5
    //创建QueryableStateClient
    val client: QueryableStateClient = new QueryableStateClient(tmHostname, proxyPort)
    //创建需要查询的状态对应的ValueStateDescriptor
    val valueDescriptor: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long]("leastValue", TypeInformation.of(new TypeHint[Long]() {}))
    //查询key为5的可查询状态值
    val resultFuture: CompletableFuture[ValueState[Long]] = client.getKvState(
      JobID.fromHexString(jobId),
      "leastQueryValue",
      key,
      Types.INT,
      valueDescriptor)
    //从resultFuture等待返回结果
    resultFuture.thenAccept(response => {
      try
        val res = response.value()
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    })
  }

}

object TemperatureDashboard {
  // assume local setup and TM runs on same machine as client
  val proxyHost = "127.0.0.1"
  val proxyPort = 9069
  // jobId of running QueryableStateJob.
  // can be looked up in logs of running job or the web UI
  val jobId = "d2447b1a5e0d952c372064c886d2220a"
  // how many sensors to query
  val numSensors = 5
  // how often to query the state
  val refreshInterval = 10000

  def main(args: Array[String]): Unit = {
    // configure client with host and port of queryable state proxy val client = new QueryableStateClient(proxyHost, proxyPort)
    val futures = new Array[
      CompletableFuture[ValueState[(String, Double)]]](numSensors)
    val results = new Array[Double](numSensors)
    // print header line of dashboard table
    val header =
      (for (i <- 0 until numSensors) yield "sensor_" + (i + 1)).mkString("\t| ")
    println(header)
    // loop forever while (true) {
    // send out async queries
    for (i <- 0 until numSensors) {
      //      futures(i) = queryState("sensor_" + (i + 1), client)
    }
    // wait for results
    for (i <- 0 until numSensors) {
      results(i) = futures(i).get().value()._2
    }
    // print result
    val line = results.map(t => f"$t%1.3f").mkString("\t| ")
    println(line)
    // wait to send out next queries
    Thread.sleep(refreshInterval)
  }

  //  client.shutdownAndWait()

  //  def queryState(key: String, client: QueryableStateClient): CompletableFuture[ValueState[(String, Double)]] = {
  //    client
  //      .getKvState[String, ValueState[(String, Double)], (String, Double)](
  //      JobID.fromHexString(jobId), "maxTemperature",
  //      key,
  //      Types.STRING,
  //      new ValueStateDescriptor[(String, Double)]("", // state name not relevant here
  //        createTypeInformation[(String, Double)]))
  //  }
}