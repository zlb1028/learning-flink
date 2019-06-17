package com.realtime.flink.metric

import com.codahale.metrics.SlidingWindowReservoir
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Counter, Histogram, Meter}

object CounterMetric {

  def main(args: Array[String]): Unit = {

  }

  class CounterMapper extends RichMapFunction[Long, Long] {
    @transient private var counter: Counter = _

    //在Open方法中获取获取Counter实例化对象
    override def open(parameters: Configuration): Unit = {
      counter = getRuntimeContext()
        .getMetricGroup()
        .counter("myCounter")
    }

    override def map(value: Long): Long = {
      if (value > 0) {
        counter.inc()
      }
      value
    }
  }

  class gaugeMapper extends RichMapFunction[String, String] {
    @transient private var countValue = 0

    //在Open方法中获取gauge实例化对象
    override def open(parameters: Configuration): Unit = {
      getRuntimeContext()
        .getMetricGroup()
        .gauge[Int, ScalaGauge[Int]]("MyGauge", ScalaGauge[Int](() => countValue))
    }

    override def map(value: String): String = {
      countValue += 1 //累加countValue值
      value
    }
  }


  class histogramMapper extends RichMapFunction[Long, Long] {
    @transient private var histogram: Histogram = _

    override def open(config: Configuration): Unit = {
      val dropwizardHistogram =
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))
      histogram = getRuntimeContext()
        .getMetricGroup()
        .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram))
    }

    override def map(value: Long): Long = {
      histogram.update(value)
      value
    }
  }

  class meterMapper extends RichMapFunction[Long,Long] {
    @transient private var meter: Meter = _
    //通过
    override def open(config: Configuration): Unit = {

      val dropwizardMeter = new com.codahale.metrics.Meter()

      meter = getRuntimeContext()
        .getMetricGroup()
        .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter))
    }

    override def map(value: Long): Long = {
      meter.markEvent()
      value
    }
  }


}


