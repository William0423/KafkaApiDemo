package com.rts.kafka.comsumer

import org.apache.spark.SparkConf

object KafkaConsumer {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("RealtimeSystemDemoApp").setMaster("local[2]")

    val Array(brokers, topicStr) = args
    val topicsSet = topicStr.split(",").toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers,
      "auto.offset.reset" -> "smallest")

  }

}
