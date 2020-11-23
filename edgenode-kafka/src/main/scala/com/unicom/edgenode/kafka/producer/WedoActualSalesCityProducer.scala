package com.unicom.edgenode.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object WedoActualSalesCityProducer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WedoActualSalesCityProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    ssc.textFile(this.getClass.getResource("/wedoactualSalesCity.log").getPath)
      .foreachPartition(partition => {
        val props = new Properties()
//        props.put("bootstrap.servers", "10.176.50.107:9092,10.176.50.108:9092,10.176.50.109:9092")
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        props.put("acks", "1")
        props.put("batch.size", "16384")
        props.put("linger.ms", "10")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item => {
          val msg = new ProducerRecord[String, String]("wedoactualsales_topic",item)
          producer.send(msg)
          println(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
