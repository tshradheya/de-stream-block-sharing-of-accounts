package com.frauddetectionaccountsharing

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

object FraudDetectionJob {


  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "kafka:9092")


    val consumer = new FlinkKafkaConsumer[String](
      "server-logs", new SimpleStringSchema(), properties
    )

    val events = env.addSource(consumer).name("incoming-events")

    val alerts = events.keyBy(event => event.split(",")(1))
      .process(new FraudDetection)
      .name("fraud-detector")


    val producer = new FlinkKafkaProducer[String]("alerts", new SimpleStringSchema(), properties)


    alerts.addSink(producer)
      .name("send-alerts")

    events
      .addSink(new ServerLogSink)
      .name("event-log")

    env.execute("Fraud Detection")
  }
}