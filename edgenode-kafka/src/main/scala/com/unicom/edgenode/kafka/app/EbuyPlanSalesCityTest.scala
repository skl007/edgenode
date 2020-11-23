package com.unicom.edgenode.kafka.app

import java.sql.ResultSet
import com.unicom.edgenode.kafka.util.{DataSourceUtil, MyKafkaUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object EbuyPlanSalesCitytest {

  //初始化消费者组
  private val groupid = "student2_consumer_origin"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("EbuyPlanSalesCity")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "5")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //将要消费的kafka的topic
    //    val topic = "so_citydif"
    val topic = "gmall_student2"
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=? and topic=?", Array(groupid, topic), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      MyKafkaUtil.getKafkaStream(groupid, topic, ssc)
    } else {
      MyKafkaUtil.getKafkaStream(groupid, topic, ssc, offsetMap)
    }


    /*val value = stream.map(item => item.value())
    value.foreachRDD(rdd=>println(rdd.collect().mkString("\n")))*/
    //解析json数据
    val ebuyDStream = stream.filter(item => item.value().split(",").length == 4).map(item => item.value()).mapPartitions(partition => {
      partition.map(item => {

        val jsonObject = ParseJsonData.getJsonData(item)
        val eventType = if (jsonObject.containsKey("eventType")) jsonObject.getString("eventType").trim else ""
        val name = if (jsonObject.containsKey("name")) jsonObject.getString("name").trim else ""
        val id = if (jsonObject.containsKey("id")) jsonObject.getString("id").trim else ""
        val age = if (jsonObject.containsKey("age")) jsonObject.getString("age").trim else ""
        (eventType, id, name, age)
      })
    })

    //    ebuyDStream.foreachRDD(rdd=>println("打印ebuyDStream: "+rdd.collect().mkString("\n")))
    //缓存处理多线程安全问题
    ebuyDStream.cache()

    //将数据存入mysql
    ebuyDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(model => {
            val eventType = model._1
            val id = model._2
            val name = model._3
            val age = model._4
            println("eventType:" + eventType + " id:" + id + " name:" + name + " age:" + age)

            //向ebuy_plansalescity表中插入或更新数据
            if (eventType == "INSERT") {
              sqlProxy.executeUpdate(client, "insert into student02(id,name,age)values(?,?,?)",
                Array(id, name, age))
            } else if (eventType == "UPDATE") {
              sqlProxy.executeUpdate(client, "update student02 set  name =? ,age =? where id=? ",
                Array(name, age, id))
            } else if (eventType == "DELETE") {
              sqlProxy.executeUpdate(client, "DELETE FROM student02 WHERE id =?",
                Array(id))
            }

          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

    })

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    println("启动流程")
    ssc.start()
    ssc.awaitTermination()

  }


}