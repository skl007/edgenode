package com.unicom.edgenode.kafka.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

object MyKafkaUtil {

  private val properties: Properties = PropertiesUtil.load("config.properties")
  val broker_list: String = properties.getProperty("kafka.broker.list")


  //创建DStream,返回接收到的输入数据
  def getKafkaStream(groupId:String,topic:String,ssc:StreamingContext):InputDStream[ConsumerRecord[String,String]]={
    //kafka消费配置
    val kafkaParam = Map[String,Object](
      "bootstrap.servers"->broker_list,
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->groupId,
      "auto.offset.reset"->"earliest",
      "enable.auto.commit"->(false:java.lang.Boolean)
    )
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam))
    KafkaDStream
  }
  def getKafkaStream(groupId:String,topic:String,ssc:StreamingContext,offsetMap:mutable.HashMap[TopicPartition, Long]):InputDStream[ConsumerRecord[String,String]]={
    //kafka消费配置
    val kafkaParam = Map[String,Object](
      "bootstrap.servers"->broker_list,
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->groupId,
      "auto.offset.reset"->"earliest",
      "enable.auto.commit"->(false:java.lang.Boolean)
    )
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam,offsetMap))
    KafkaDStream
  }
}
