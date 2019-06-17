package com.realtime.flink.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment,_}

/**
  * Created by zhanglibing on 2019/3/13
  */
object BulckIterations {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 创建初始化数据集
    val initial = env.fromElements(0)
    //调用迭代方法，并设定迭代次数为10000次
    val count = initial.iterate(10000) { iterationInput: DataSet[Int] =>
      val result = iterationInput.map { i =>
        val x = Math.random()
        val y = Math.random()
        i + (if (x * x + y * y < 1) 1 else 0)
      }
      result
    }
    //输出迭代结果
    val result = count map { c => c / 10000.0 * 4 }

    result.print()

    env.execute("Iterative Pi Example")
  }

}
